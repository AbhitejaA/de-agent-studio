# =============================================================================
# Pipeline  : top_10_subscribers_by_watch_time
# Layer     : Gold
# Schedule  : Daily @ 07:00 UTC  (0 7 * * *)
# Author    : Senior Data Engineering – Peacock TV
#
# QA Fixes Applied
# ────────────────
# CRITICAL-1 : 'run_date' added to NOTEBOOK_PARAMS using {{ ds }} so the
#              Airflow execution date is explicitly forwarded to the notebook.
#              Backfill and late-running DAGs now always target the correct
#              partition.
# CRITICAL-2 : retries / retry_delay are now resolved from the YAML config
#              for the active environment at DAG parse time, so prod gets 3
#              retries (not 2).
# WARNING-1  : timeout_seconds is resolved per environment from config
#              (dev=1800, stage=2700, prod=3600) via ENV-keyed lookup.
# WARNING-2  : num_workers is resolved per environment from config
#              (dev=1, stage=2, prod=4) — prod was under-provisioned at 2.
# WARNING-3  : _slack_alert() now uses SlackWebhookHook directly instead of
#              instantiating SlackWebhookOperator inside a callback, which is
#              an unsupported Airflow pattern.
# SUGGESTION-1: SLA miss configured on run_notebook task (sla=timedelta(hours=1)).
# SUGGESTION-2: on_success_callback added to post green confirmation to Slack.
# QA Fix (critical): top_n forwarded via NOTEBOOK_PARAMS from config so the
#              notebook never hardcodes the value.
# =============================================================================

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

log = logging.getLogger(__name__)

# ── Environment resolution ─────────────────────────────────────────────────────
ENV: str = os.getenv("AIRFLOW_ENV", "dev")  # dev | stage | prod

# ── Load YAML config at DAG parse time ────────────────────────────────────────
_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "top_10_subscribers_by_watch_time_config.yml")

with open(_CONFIG_PATH, "r") as _fh:
    _CFG = yaml.safe_load(_fh)

_PIPELINE   = _CFG["pipeline"]
_ENV_CFG    = _CFG["environments"][ENV]
_ALERT_CFG  = _CFG["alerting"]
_QG_CFG     = _CFG["quality_gates"]
_CLUSTER    = _ENV_CFG["cluster"]

# ── Config-driven constants ────────────────────────────────────────────────────
PIPELINE_NAME       : str = _PIPELINE["name"]
OUTPUT_TABLE        : str = _PIPELINE["output_table"]
TOP_N               : int = _PIPELINE["top_n"]

# QA Fix (critical): retries / retry_delay from YAML env block — not hardcoded.
RETRIES             : int           = _ENV_CFG["retries"]
RETRY_DELAY         : timedelta     = timedelta(minutes=_ENV_CFG["retry_delay_minutes"])

# QA Fix (warning): timeout resolved per environment from config.
TIMEOUT_SECONDS     : int           = _ENV_CFG["timeout_seconds"]

# QA Fix (warning): num_workers resolved per environment from config.
NUM_WORKERS         : int           = _CLUSTER["num_workers"]

DATABRICKS_CONN_ID  : str           = _ENV_CFG["databricks_conn_id"]
SLACK_CONN_ID       : str           = _ALERT_CFG["slack_webhook_conn_id"]
SLACK_CHANNEL       : str           = _ENV_CFG["slack_channel"]
NOTIFY_ON           : list[str]     = _ALERT_CFG["notify_on"]

NOTEBOOK_PATH       : str = f"/Shared/peacock/gold/{PIPELINE_NAME}"

# QA Fix (critical): run_date uses {{ ds }} (Airflow execution date) so backfills
# and late-running DAGs always target the correct partition — never wall-clock.
# QA Fix (config drift): top_n sourced from config, passed as widget parameter.
NOTEBOOK_PARAMS: dict[str, str] = {
    "run_date": "{{ ds }}",                 # ← CRITICAL FIX: Airflow logical date
    "top_n"   : str(TOP_N),                 # ← single source of truth from YAML
    "env"     : ENV,
}

CLUSTER_CONFIG: dict = {
    "spark_version"            : _CLUSTER["spark_version"],
    "node_type_id"             : _CLUSTER["node_type_id"],
    "num_workers"              : NUM_WORKERS,            # QA Fix (warning)
    "auto_termination_minutes" : _CLUSTER["auto_termination_minutes"],
    "spark_conf": {
        "spark.databricks.delta.optimizeWrite.enabled"  : "true",
        "spark.databricks.delta.autoCompact.enabled"    : "true",
    },
}


# ── Slack alert helpers ────────────────────────────────────────────────────────
# QA Fix (warning): Uses SlackWebhookHook directly — not SlackWebhookOperator —
# which is the correct, supported pattern for Airflow callback functions.

def _build_slack_message(context: dict, event: str) -> str:
    """Build a formatted Slack message for the given pipeline event."""
    icons   = {"failure": ":red_circle:", "retry": ":yellow_circle:", "success": ":large_green_circle:"}
    icon    = icons.get(event, ":white_circle:")
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_dt = context["ds"]
    log_url = context["task_instance"].log_url
    return (
        f"{icon} *Peacock Pipeline {event.upper()}*\n"
        f">*DAG*      : `{dag_id}`\n"
        f">*Task*     : `{task_id}`\n"
        f">*Exec Date*: `{exec_dt}`\n"
        f">*Env*      : `{ENV}`\n"
        f">*Logs*     : <{log_url}|View Logs>"
    )


def _send_slack(context: dict, event: str) -> None:
    """Send a Slack notification via SlackWebhookHook (callback-safe pattern)."""
    if event not in NOTIFY_ON:
        return
    try:
        hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
        hook.send(
            text=_build_slack_message(context, event),
            channel=SLACK_CHANNEL,
        )
    except Exception as exc:  # never let alerting failures crash the callback
        log.error("Slack %s notification failed: %s", event, exc)


def _on_failure_callback(context: dict) -> None:
    _send_slack(context, "failure")


def _on_retry_callback(context: dict) -> None:
    _send_slack(context, "retry")


def _on_success_callback(context: dict) -> None:
    # QA Fix (suggestion): success notification so operators know the Gold
    # table was populated — not only alerted on failure/retry.
    _send_slack(context, "success")


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:  # noqa: ANN001
    """Called by Airflow when the SLA window is breached."""
    # QA Fix (suggestion): SLA miss posts a distinct alert separate from failures.
    try:
        hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
        hook.send(
            text=(
                f":alarm_clock: *SLA MISS – Peacock Pipeline*\n"
                f">*DAG* : `{dag.dag_id}`\n"
                f">*Env* : `{ENV}`\n"
                f">Tasks still running: `{[t.task_id for t in blocking_tis]}`"
            ),
            channel=SLACK_CHANNEL,
        )
    except Exception as exc:
        log.error("Slack SLA miss notification failed: %s", exc)


# ── Default args ───────────────────────────────────────────────────────────────
default_args: dict = {
    "owner"             : "data-engineering",
    "depends_on_past"   : False,
    "start_date"        : datetime(2024, 1, 1),
    "email_on_failure"  : False,
    "email_on_retry"    : False,
    # QA Fix (critical): retries/retry_delay loaded from YAML env block.
    "retries"           : RETRIES,
    "retry_delay"       : RETRY_DELAY,
    "on_failure_callback": _on_failure_callback,
    "on_retry_callback" : _on_retry_callback,
}

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id              = PIPELINE_NAME,
    default_args        = default_args,
    description         = (
        "Gold layer pipeline: top-10 subscribers by watch time (daily). "
        f"Env={ENV}"
    ),
    schedule_interval   = _PIPELINE["schedule"],   # "0 7 * * *"
    catchup             = True,     # support backfills — run_date comes from {{ ds }}
    max_active_runs     = 1,        # prevent overlapping runs on the same partition
    tags                = ["gold", "subscribers", "watch-time", ENV],
    # QA Fix (suggestion): SLA miss callback on the DAG.
    sla_miss_callback   = _sla_miss_callback,
) as dag:

    # ── Sentinel: start ───────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Core: submit notebook to Databricks ───────────────────────────────────
    run_notebook = DatabricksSubmitRunOperator(
        task_id             = "run_notebook",
        databricks_conn_id  = DATABRICKS_CONN_ID,
        # QA Fix (warning): timeout_seconds resolved per environment from config.
        timeout_seconds     = TIMEOUT_SECONDS,
        # QA Fix (suggestion): SLA miss fires if task not done within 1 hour.
        sla                 = timedelta(hours=_ALERT_CFG["sla_minutes"] / 60),
        on_success_callback = _on_success_callback,   # QA Fix (suggestion)
        notebook_task       = {
            "notebook_path"     : NOTEBOOK_PATH,
            "base_parameters"   : NOTEBOOK_PARAMS,   # includes run_date + top_n
        },
        new_cluster         = CLUSTER_CONFIG,
    )

    # ── Sentinel: end ─────────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── Task dependency chain ─────────────────────────────────────────────────
    start >> run_notebook >> end
