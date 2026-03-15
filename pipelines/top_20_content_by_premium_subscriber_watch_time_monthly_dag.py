"""
============================================================
DAG  : top_20_content_by_premium_subscriber_watch_time_monthly
Owner: Peacock Data Engineering
Schedule : 0 8 1 * *  — 08:00 UTC on the 1st of every month
Purpose  : Ranks top-20 content titles by total watch-time
           minutes for PREMIUM + active subscribers over the
           previous full calendar month, writing results to
           gold_top20_content_by_premium_watch_time_monthly.
============================================================
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# ──────────────────────────────────────────────
# ENVIRONMENT / CONFIG RESOLUTION
# ──────────────────────────────────────────────
ENV = os.getenv("AIRFLOW_ENV", "dev")  # dev | stage | prod

ENVIRONMENT_CONFIG: dict[str, dict] = {
    "dev": {
        "databricks_conn_id": "databricks_dev",
        "cluster_id":         "0101-dev-cluster",
        "notebook_path":      "/Repos/peacock/dev/pipelines/top_20_content_by_premium_subscriber_watch_time_monthly_notebook",
        "slack_conn_id":      "slack_webhook_dev",
        "slack_channel":      "#data-alerts-dev",
    },
    "stage": {
        "databricks_conn_id": "databricks_stage",
        "cluster_id":         "0101-stage-cluster",
        "notebook_path":      "/Repos/peacock/stage/pipelines/top_20_content_by_premium_subscriber_watch_time_monthly_notebook",
        "slack_conn_id":      "slack_webhook_stage",
        "slack_channel":      "#data-alerts-stage",
    },
    "prod": {
        "databricks_conn_id": "databricks_prod",
        "cluster_id":         "0101-prod-cluster",
        "notebook_path":      "/Repos/peacock/prod/pipelines/top_20_content_by_premium_subscriber_watch_time_monthly_notebook",
        "slack_conn_id":      "slack_webhook_prod",
        "slack_channel":      "#data-alerts-prod",
    },
}

cfg = ENVIRONMENT_CONFIG[ENV]

# ──────────────────────────────────────────────
# SLACK ALERT HELPERS
# ──────────────────────────────────────────────
def _slack_failure_alert(context: dict) -> None:
    """Send Slack notification on task failure."""
    SlackWebhookOperator(
        task_id="slack_failure_notification",
        slack_webhook_conn_id=cfg["slack_conn_id"],
        channel=cfg["slack_channel"],
        message=(
            ":red_circle: *FAILURE* | DAG: `{dag}`  Task: `{task}`\n"
            ">*Env*: `{env}`  |  *Run*: `{run_id}`\n"
            ">*Execution Date*: `{exec_date}`\n"
            ">*Log*: {log_url}"
        ).format(
            dag=context["dag"].dag_id,
            task=context["task_instance"].task_id,
            env=ENV,
            run_id=context["run_id"],
            exec_date=context["logical_date"],
            log_url=context["task_instance"].log_url,
        ),
    ).execute(context)


def _slack_retry_alert(context: dict) -> None:
    """Send Slack notification on task retry."""
    SlackWebhookOperator(
        task_id="slack_retry_notification",
        slack_webhook_conn_id=cfg["slack_conn_id"],
        channel=cfg["slack_channel"],
        message=(
            ":large_yellow_circle: *RETRY* | DAG: `{dag}`  Task: `{task}`\n"
            ">*Env*: `{env}`  |  *Attempt*: `{attempt}`\n"
            ">*Execution Date*: `{exec_date}`\n"
            ">*Log*: {log_url}"
        ).format(
            dag=context["dag"].dag_id,
            task=context["task_instance"].task_id,
            env=ENV,
            attempt=context["task_instance"].try_number,
            exec_date=context["logical_date"],
            log_url=context["task_instance"].log_url,
        ),
    ).execute(context)


# ──────────────────────────────────────────────
# DEFAULT ARGS
# ──────────────────────────────────────────────
DEFAULT_ARGS: dict = {
    "owner":            "peacock-data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "on_failure_callback": _slack_failure_alert,
    "on_retry_callback":   _slack_retry_alert,
}

# ──────────────────────────────────────────────
# NOTEBOOK WIDGET PARAMETERS
# Airflow's {{ macros }} resolve at runtime so the
# notebook always receives the previous full calendar month.
# ──────────────────────────────────────────────
NOTEBOOK_PARAMS: dict[str, str] = {
    # First day of the previous month  →  e.g. 2024-04-01
    "month_start_date": "{{ macros.ds_format(macros.dateutil.relativedelta.relativedelta(months=-1) | string, '%Y-%m-%d', '%Y-%m-01') }}",
    # Last day of the previous month   →  e.g. 2024-04-30
    "month_end_date":   "{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
    "environment":      ENV,
    "output_table":     "gold_top20_content_by_premium_watch_time_monthly",
}

# ──────────────────────────────────────────────
# DAG DEFINITION
# ──────────────────────────────────────────────
with DAG(
    dag_id="top_20_content_by_premium_subscriber_watch_time_monthly",
    description=(
        "Ranks top-20 content by premium-subscriber watch-time "
        "for the previous full calendar month and writes to Gold Delta."
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="0 8 1 * *",
    catchup=False,
    max_active_runs=1,
    tags=["gold", "content", "premium", "monthly", "peacock"],
) as dag:

    # ── Sentinel start ──────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Core Databricks notebook run ────────────
    run_notebook = DatabricksSubmitRunOperator(
        task_id="run_top20_premium_watch_time_notebook",
        databricks_conn_id=cfg["databricks_conn_id"],
        existing_cluster_id=cfg["cluster_id"],
        notebook_task={
            "notebook_path":       cfg["notebook_path"],
            "base_parameters":     NOTEBOOK_PARAMS,
            "source":              "WORKSPACE",
        },
        # Surfaced in the Databricks Jobs UI for traceability
        run_name=(
            "top_20_content_by_premium_subscriber_watch_time_monthly"
            "__{{ ds }}"
        ),
        on_failure_callback=_slack_failure_alert,
        on_retry_callback=_slack_retry_alert,
    )

    # ── Sentinel end ────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── Task dependencies ───────────────────────
    start >> run_notebook >> end
