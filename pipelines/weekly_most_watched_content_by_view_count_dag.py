# =============================================================================
# Pipeline  : weekly_most_watched_content_by_view_count
# DAG File  : weekly_most_watched_content_by_view_count_dag.py
# Schedule  : Every Monday at 06:00 UTC  (cron: 0 6 * * 1)
# Layer     : Gold
# Maintained: Peacock Data Engineering
#
# QA FIXES APPLIED
# ----------------
# Critical #4 : Slack alert now resolves context["ds"] at runtime instead of
#               rendering the literal string '{{ ds }}' from an f-string.
# Warning  W1  : start_date is a fixed absolute datetime(2025, 1, 6) — a known
#               Monday — not the anti-pattern days_ago(1).
# Warning  W2  : 'env' Variable lookup uses a dedicated key 'pipeline_env'.
#               A DAG-level assertion validates the variable is present before
#               the Databricks task is submitted, preventing silent prod fallback.
# Warning  W6  : notebook_path is read from the YAML config per environment
#               rather than a hardcoded Python constant.
# Warning  W5  : spark_version is read from the YAML config (single source of
#               truth); the DAG no longer hardcodes it independently.
# =============================================================================

from __future__ import annotations

import os
from datetime import datetime

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# ---------------------------------------------------------------------------
# 1. Resolve environment & load YAML config
# ---------------------------------------------------------------------------
# WARNING W2 FIX: Use a dedicated Airflow Variable key.
# If 'pipeline_env' is not set, we raise immediately rather than silently
# defaulting to prod — preventing accidental writes to production tables.
_RAW_ENV = Variable.get("pipeline_env", default_var=None)

if _RAW_ENV is None:
    raise EnvironmentError(
        "[weekly_most_watched_content_by_view_count] Airflow Variable "
        "'pipeline_env' is not set.  Please define it (dev | stage | prod) "
        "before enabling this DAG.  Refusing to default to prod to prevent "
        "unintended writes to production Gold tables."
    )

ENV: str = _RAW_ENV.strip().lower()
if ENV not in {"dev", "stage", "prod"}:
    raise ValueError(
        f"[weekly_most_watched_content_by_view_count] 'pipeline_env' must be "
        f"one of 'dev', 'stage', 'prod'.  Got: '{ENV}'."
    )

# Load the YAML config — path is relative to this DAG file
_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "weekly_most_watched_content_by_view_count_config.yml",
)
with open(_CONFIG_PATH, "r") as _f:
    _FULL_CONFIG = yaml.safe_load(_f)

# Merge defaults → environment overrides (YAML anchors already handled by
# the parser, but we keep an explicit reference for clarity)
CFG: dict = _FULL_CONFIG[ENV]

# ---------------------------------------------------------------------------
# 2. Config-driven constants
# ---------------------------------------------------------------------------
PIPELINE_NAME:      str = CFG["pipeline_name"]
NOTEBOOK_PATH:      str = CFG["notebook_path"]          # WARNING W6 FIX
SPARK_VERSION:      str = CFG["spark_version"]           # WARNING W5 FIX
NODE_TYPE_ID:       str = CFG["node_type_id"]
NUM_WORKERS:        int = CFG["num_workers"]
AUTOTERMINATION:    int = CFG["autotermination_minutes"]
DATABRICKS_CONN_ID: str = CFG["databricks_conn_id"]
SLACK_CHANNEL:      str = CFG["slack_channel"]
SLACK_CONN_ID:      str = CFG["slack_connection_id"]
DATABASE:           str = CFG["database"]
OUTPUT_TABLE:       str = CFG["output_table"]
SESSIONS_TABLE:     str = CFG["sessions_table_input"]
CATALOG_TABLE:      str = CFG["catalog_table_input"]
VACUUM_HOURS:       int = CFG["vacuum_retain_hours"]
TOP_N              = CFG.get("top_n")                   # None or int

# ---------------------------------------------------------------------------
# 3. Slack alert helpers
# ---------------------------------------------------------------------------

def _build_slack_message(context: dict, status: str, emoji: str) -> str:
    """
    Build a structured Slack message from the Airflow task context.

    CRITICAL FIX #4
    ---------------
    Previously used an f-string literal '{{ ds }}' which rendered as the
    literal text '{{ ds }}' in Slack rather than the real execution date.
    Now resolves context["ds"] at runtime so alerts show the actual date.
    """
    # Safely extract values — provide fallbacks so the alert never crashes
    dag_id        = context.get("dag").dag_id       if context.get("dag")  else PIPELINE_NAME
    task_id       = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
    exec_date     = context.get("ds", "unknown-date")          # CRITICAL FIX #4
    log_url       = context.get("task_instance").log_url if context.get("task_instance") else "N/A"
    run_id        = context.get("run_id", "unknown-run")

    return (
        f"{emoji} *Peacock Data — {PIPELINE_NAME}*\n"
        f">*Status*:          `{status}`\n"
        f">*DAG*:             `{dag_id}`\n"
        f">*Task*:            `{task_id}`\n"
        f">*Execution Date*:  `{exec_date}`\n"      # real date, not literal
        f">*Run ID*:          `{run_id}`\n"
        f">*Environment*:     `{ENV}`\n"
        f">*Output Table*:    `{DATABASE}.{OUTPUT_TABLE}`\n"
        f">*Log URL*:         {log_url}"
    )


def _on_failure_callback(context: dict) -> None:
    """Send a Slack alert when any task in the DAG fails."""
    msg = _build_slack_message(context, status="FAILED ❌", emoji=":red_circle:")
    SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id=SLACK_CONN_ID,
        channel=SLACK_CHANNEL,
        message=msg,
    ).execute(context)


def _on_retry_callback(context: dict) -> None:
    """Send a Slack alert when any task is retried."""
    msg = _build_slack_message(context, status="RETRYING ⚠️", emoji=":large_yellow_circle:")
    SlackWebhookOperator(
        task_id="slack_retry_alert",
        slack_webhook_conn_id=SLACK_CONN_ID,
        channel=SLACK_CHANNEL,
        message=msg,
    ).execute(context)


def _on_success_callback(context: dict) -> None:
    """Send a Slack notification on successful DAG completion."""
    msg = _build_slack_message(context, status="SUCCESS ✅", emoji=":large_green_circle:")
    SlackWebhookOperator(
        task_id="slack_success_alert",
        slack_webhook_conn_id=SLACK_CONN_ID,
        channel=SLACK_CHANNEL,
        message=msg,
    ).execute(context)


# ---------------------------------------------------------------------------
# 4. Default DAG arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner":               "peacock-data-engineering",
    "depends_on_past":     False,
    # WARNING W1 FIX: Fixed absolute start_date on a known Monday (2025-01-06).
    # Never use days_ago() — it shifts with every scheduler parse cycle and
    # produces non-deterministic first-run behaviour across deployments.
    "start_date":          datetime(2025, 1, 6),
    "email_on_failure":    False,
    "email_on_retry":      False,
    "retries":             2,
    "retry_delay":         __import__("datetime").timedelta(minutes=5),
    "on_failure_callback": _on_failure_callback,
    "on_retry_callback":   _on_retry_callback,
}

# ---------------------------------------------------------------------------
# 5. Cluster definition (sourced entirely from YAML config)
# ---------------------------------------------------------------------------
NEW_CLUSTER = {
    "spark_version":            SPARK_VERSION,   # WARNING W5 FIX — from YAML
    "node_type_id":             NODE_TYPE_ID,
    "num_workers":              NUM_WORKERS,
    "autotermination_minutes":  AUTOTERMINATION,
    "spark_conf": {
        # Partition-safe overwrite — only the affected week_start_date
        # partition is replaced; historical partitions are untouched.
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
    },
    "custom_tags": {
        "pipeline":    PIPELINE_NAME,
        "environment": ENV,
        "layer":       "gold",
        "team":        "peacock-data-engineering",
    },
}

# ---------------------------------------------------------------------------
# 6. Notebook parameters passed to the Databricks run
# ---------------------------------------------------------------------------
NOTEBOOK_PARAMS = {
    "execution_date":       "{{ ds }}",          # Airflow template — resolved at runtime
    "env":                  ENV,
    "sessions_table":       SESSIONS_TABLE,
    "catalog_table":        CATALOG_TABLE,
    "output_table":         OUTPUT_TABLE,
    "database":             DATABASE,
    "vacuum_retain_hours":  str(VACUUM_HOURS),   # widgets are strings
    "top_n":                str(TOP_N) if TOP_N is not None else "null",
}

# ---------------------------------------------------------------------------
# 7. DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id=f"{PIPELINE_NAME}_{ENV}",
    description=(
        "Gold pipeline — ranks content by distinct view count for the previous "
        "completed ISO week.  Writes to Delta table partitioned by week_start_date."
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * 1",    # Every Monday at 06:00 UTC
    catchup=False,
    max_active_runs=1,                # Prevent concurrent weekly runs overlapping
    tags=["gold", "content", "weekly", ENV, "peacock"],
    on_success_callback=_on_success_callback,
) as dag:

    # -------------------------------------------------------------------------
    # Task 1 — Submit Databricks notebook run
    # -------------------------------------------------------------------------
    run_notebook = DatabricksSubmitRunOperator(
        task_id="run_weekly_most_watched_content_notebook",
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster=NEW_CLUSTER,
        notebook_task={
            "notebook_path":        NOTEBOOK_PATH,   # WARNING W6 FIX — from YAML
            "base_parameters":      NOTEBOOK_PARAMS,
            "source":               "WORKSPACE",
        },
        # DatabricksSubmitRunOperator will poll until the run completes
        # (or the cluster auto-terminates at AUTOTERMINATION minutes).
        polling_period_seconds=60,
        do_xcom_push=False,
    )

    run_notebook  # Single-task DAG; extend here with sensors/validation tasks
