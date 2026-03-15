"""
DAG  : daily_churned_subscribers_30day_inactivity
Layer: Gold
Owner: Peacock Data Engineering
Schedule: 0 6 * * *  (06:00 UTC daily)

FIX APPLIED (QA Issue):
    Jinja templates inside a raw dict passed to json= are NOT rendered by
    Airflow at task execution time — they are passed as literal strings to the
    Databricks API.
    FIX: Resolve all dynamic values (ds, environment) at the DAG/task level
    using Airflow Variables and Python callables via PythonOperator +
    TriggerDagRunOperator pattern, OR — the safest supported path — resolve
    environment via an Airflow Variable at module parse time (static) and pass
    run_date through notebook base_parameters using the operator's confirmed
    templated_fields so Jinja IS rendered for the `json` field in
    DatabricksSubmitRunOperator (supported since apache-airflow-providers-
    databricks >= 3.3.0, where `json` is listed in templated_fields).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# ---------------------------------------------------------------------------
# Environment resolution
# ---------------------------------------------------------------------------
# Resolved once at task render time via Airflow Variable (not inside json dict)
# so it is safe and deterministic.
ENV = Variable.get("environment", default_var="dev")

CATALOG_MAP = {
    "dev":   "dev_catalog",
    "stage": "stage_catalog",
    "prod":  "prod_catalog",
}
SCHEMA = "peacock_core"
CATALOG = CATALOG_MAP.get(ENV, "dev_catalog")

DATABRICKS_CONN_ID = "databricks_default"
NOTEBOOK_PATH = f"/Repos/peacock-pipelines/{ENV}/notebooks/daily_churned_subscribers_30day_inactivity_notebook"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# Slack callback helpers
# ---------------------------------------------------------------------------
def _slack_alert(context: dict, event: str) -> None:
    """Send a Slack alert on task failure or retry."""
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        dag_id  = context["dag"].dag_id
        task_id = context["task_instance"].task_id
        run_id  = context["run_id"]
        log_url = context["task_instance"].log_url

        emoji   = ":red_circle:" if event == "failure" else ":large_yellow_circle:"
        message = (
            f"{emoji} *Peacock Pipeline {event.upper()}*\n"
            f">*DAG*: `{dag_id}`\n"
            f">*Task*: `{task_id}`\n"
            f">*Run ID*: `{run_id}`\n"
            f">*Env*: `{ENV}`\n"
            f">*Logs*: <{log_url}|View Logs>"
        )
        slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_data_alerts")
        slack_hook.send(text=message)
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] Slack alert failed: {exc}")


def on_failure_callback(context: dict) -> None:
    _slack_alert(context, "failure")


def on_retry_callback(context: dict) -> None:
    _slack_alert(context, "retry")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="daily_churned_subscribers_30day_inactivity",
    default_args=default_args,
    description="Identify churned subscribers with 30+ days of inactivity and write to gold layer.",
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["gold", "churn", "subscribers", ENV],
    on_failure_callback=on_failure_callback,
) as dag:

    # -----------------------------------------------------------------------
    # NOTE ON JINJA FIX:
    #   `json` IS a templated field in DatabricksSubmitRunOperator
    #   (apache-airflow-providers-databricks >= 3.3.0).
    #   We use {{ ds }} inside the json dict string so Airflow renders it at
    #   execution time.  Environment/catalog are resolved as Python strings
    #   OUTSIDE the dict (at parse/render time via Variable.get) so they are
    #   always concrete values — never unrendered Jinja inside a nested dict
    #   key that Airflow cannot reach.
    #
    #   idempotency_token uses a plain Python f-string (ENV is a module-level
    #   string) combined with {{ ds }} Jinja for the date portion, placed in
    #   a top-level templated string field so it renders correctly.
    # -----------------------------------------------------------------------

    run_churn_pipeline = DatabricksSubmitRunOperator(
        task_id="run_daily_churned_subscribers_30day_inactivity",
        databricks_conn_id=DATABRICKS_CONN_ID,
        on_failure_callback=on_failure_callback,
        on_retry_callback=on_retry_callback,
        # `json` is in templated_fields → {{ ds }} WILL be rendered by Airflow
        json={
            "run_name": f"daily_churned_subscribers_30day_inactivity_{ENV}_{{{{ ds }}}}",
            "idempotency_token": f"daily_churned_subscribers_30day_inactivity_{ENV}_{{{{ ds }}}}",
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 4,
                "spark_conf": {
                    "spark.sql.shuffle.partitions": "200",
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled": "true",
                },
                "aws_attributes": {
                    "availability": "SPOT_WITH_FALLBACK",
                    "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                    "ebs_volume_count": 1,
                    "ebs_volume_size": 100,
                },
            },
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATH,
                # base_parameters are passed to dbutils.widgets in the notebook.
                # {{ ds }} is rendered by Airflow because `json` is a templated field.
                "base_parameters": {
                    "run_date": "{{ ds }}",
                    "env":      ENV,
                    "catalog":  CATALOG,
                    "schema":   SCHEMA,
                },
            },
            "libraries": [
                {"pypi": {"package": "delta-spark==2.4.0"}},
            ],
            "timeout_seconds": 3600,
            "max_retries": 2,
            "min_retry_interval_millis": 300000,
        },
    )

    run_churn_pipeline  # single-task DAG; extend here for downstream dependencies
