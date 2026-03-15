# Databricks notebook source
# ============================================================
# Notebook : top_20_content_by_premium_subscriber_watch_time_monthly
# Output   : gold_top20_content_by_premium_watch_time_monthly
# Schedule : 0 8 1 * *  (08:00 UTC, 1st of every month)
# Author   : Peacock Data Engineering
# ============================================================
# MAGIC %md
# ## Top-20 Content by Premium Subscriber Watch-Time (Monthly)
# Reads `viewing_sessions`, `subscriber_profiles`, and
# `content_catalog`; produces the top-20 ranked content titles
# for PREMIUM + active subscribers over the previous full
# calendar month. Writes a partitioned Gold Delta table.
# ============================================================

# COMMAND ----------
# ── 0. IMPORTS & SPARK SESSION ──────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------
# ── 1. WIDGET PARAMETERS (injected by Airflow DAG) ──────────
dbutils.widgets.text("month_start_date", "")   # e.g. 2024-04-01
dbutils.widgets.text("month_end_date",   "")   # e.g. 2024-04-30
dbutils.widgets.text("environment",      "dev")
dbutils.widgets.text("output_table",     "gold_top20_content_by_premium_watch_time_monthly")

month_start_date_str = dbutils.widgets.get("month_start_date")
month_end_date_str   = dbutils.widgets.get("month_end_date")
environment          = dbutils.widgets.get("environment")
output_table         = dbutils.widgets.get("output_table")

# ── Derive dates programmatically if widgets are empty (local dev) ──
if not month_start_date_str or not month_end_date_str:
    today            = date.today()
    first_of_month   = today.replace(day=1)
    prev_month_end   = first_of_month - relativedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)
    month_start_date_str = prev_month_start.strftime("%Y-%m-%d")
    month_end_date_str   = prev_month_end.strftime("%Y-%m-%d")
    logger.warning("Widgets empty — derived dates: %s → %s", month_start_date_str, month_end_date_str)

month_start_date = datetime.strptime(month_start_date_str, "%Y-%m-%d").date()
month_end_date   = datetime.strptime(month_end_date_str,   "%Y-%m-%d").date()

logger.info("Pipeline parameters | env=%s | month_start=%s | month_end=%s | output=%s",
            environment, month_start_date, month_end_date, output_table)

# COMMAND ----------
# ── 2. ENVIRONMENT → CATALOG / SCHEMA ───────────────────────
ENV_MAP = {
    "dev":   {"catalog": "peacock_dev",   "schema": "gold"},
    "stage": {"catalog": "peacock_stage", "schema": "gold"},
    "prod":  {"catalog": "peacock_prod",  "schema": "gold"},
}
env_cfg         = ENV_MAP.get(environment, ENV_MAP["dev"])
catalog         = env_cfg["catalog"]
schema          = env_cfg["schema"]
full_output_tbl = f"{catalog}.{schema}.{output_table}"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

logger.info("Target table → %s", full_output_tbl)

# COMMAND ----------
# ── 3. SQL QUERY (single-pass; params bound via spark.sql) ───
SQL = f"""
WITH

-- 3-A. PREMIUM ACTIVE SUBSCRIBERS
premium_subscribers AS (
    SELECT
        subscriber_id,
        subscription_plan,
        is_active
    FROM subscriber_profiles
    WHERE LOWER(subscription_plan) = 'premium'
      AND is_active = TRUE
),

-- 3-B. VALID SESSIONS — scoped to previous calendar month
--      NULL / zero duration_minutes are excluded
filtered_sessions AS (
    SELECT
        vs.session_id,
        vs.subscriber_id,
        vs.content_id,
        vs.duration_minutes,
        vs.session_start_date
    FROM viewing_sessions AS vs
    INNER JOIN premium_subscribers AS ps
        ON vs.subscriber_id = ps.subscriber_id
    WHERE vs.session_start_date >= DATE '{month_start_date_str}'
      AND vs.session_start_date <  DATE_ADD(DATE '{month_end_date_str}', 1)
      AND vs.duration_minutes IS NOT NULL
      AND vs.duration_minutes > 0
),

-- 3-C. AGGREGATE PER CONTENT
content_aggregates AS (
    SELECT
        content_id,
        SUM(duration_minutes)         AS total_watch_time_minutes,
        COUNT(DISTINCT subscriber_id) AS unique_viewers
    FROM filtered_sessions
    GROUP BY content_id
),

-- 3-D. ENRICH WITH CONTENT CATALOG METADATA (active only)
enriched_content AS (
    SELECT
        ca.content_id,
        ca.title,
        ca.genre,
        ca.content_type,
        ca.release_year,
        agg.total_watch_time_minutes,
        agg.unique_viewers
    FROM content_aggregates AS agg
    INNER JOIN content_catalog AS ca
        ON agg.content_id = ca.content_id
    WHERE ca.is_active = TRUE
),

-- 3-E. DUAL RANKING
--      DENSE_RANK  → tie-safe rank by watch-time DESC
--      ROW_NUMBER  → deterministic cut (content_id ASC tie-break)
ranked_content AS (
    SELECT
        content_id,
        title,
        genre,
        content_type,
        release_year,
        total_watch_time_minutes,
        unique_viewers,
        DENSE_RANK()  OVER (
            ORDER BY total_watch_time_minutes DESC
        )                               AS watch_time_dense_rank,
        ROW_NUMBER()  OVER (
            ORDER BY total_watch_time_minutes DESC,
                     content_id          ASC
        )                               AS row_num
    FROM enriched_content
)

-- 3-F. FINAL OUTPUT — TOP 20
SELECT
    DATE '{month_start_date_str}'           AS month_start_date,
    row_num                                 AS rank_position,
    watch_time_dense_rank,
    content_id,
    title,
    genre,
    content_type,
    release_year,
    total_watch_time_minutes,
    unique_viewers,
    CURRENT_TIMESTAMP()                     AS pipeline_run_timestamp
FROM ranked_content
WHERE row_num <= 20
ORDER BY row_num ASC
"""

logger.info("Executing ranking SQL for month_start=%s", month_start_date_str)
result_df = spark.sql(SQL)

# COMMAND ----------
# ── 4. DATA QUALITY CHECKS ───────────────────────────────────
logger.info("Running data quality checks ...")

row_count = result_df.count()
assert row_count == 20, (
    f"DQ FAILED: Expected exactly 20 rows, got {row_count}. "
    f"Check content_catalog active entries and premium session volume for {month_start_date_str}."
)

null_content_ids = result_df.filter(F.col("content_id").isNull()).count()
assert null_content_ids == 0, \
    f"DQ FAILED: {null_content_ids} NULL content_id(s) found in output."

null_watch_time = result_df.filter(
    F.col("total_watch_time_minutes").isNull() | (F.col("total_watch_time_minutes") <= 0)
).count()
assert null_watch_time == 0, \
    f"DQ FAILED: {null_watch_time} row(s) with NULL or non-positive total_watch_time_minutes."

rank_out_of_range = result_df.filter(
    (F.col("rank_position") < 1) | (F.col("rank_position") > 20)
).count()
assert rank_out_of_range == 0, \
    f"DQ FAILED: {rank_out_of_range} row(s) with rank_position outside [1, 20]."

duplicate_ranks = result_df.groupBy("rank_position").count().filter(F.col("count") > 1).count()
assert duplicate_ranks == 0, \
    f"DQ FAILED: {duplicate_ranks} duplicate rank_position value(s) found."

null_month = result_df.filter(F.col("month_start_date").isNull()).count()
assert null_month == 0, \
    f"DQ FAILED: {null_month} NULL month_start_date value(s) found."

logger.info("All data quality checks passed. ✓")

# COMMAND ----------
# ── 5. WRITE TO GOLD DELTA TABLE ─────────────────────────────
logger.info("Writing results to %s (partitioned by month_start_date) ...", full_output_tbl)

(
    result_df.write
    .format("delta")
    .mode("overwrite")                              # idempotent monthly overwrite
    .option("overwriteSchema", "false")             # protect schema from accidental drift
    .option("replaceWhere", f"month_start_date = DATE '{month_start_date_str}'")
    .partitionBy("month_start_date")
    .saveAsTable(full_output_tbl)
)

logger.info("Write complete. ✓")

# COMMAND ----------
# ── 6. OPTIMIZE + ZORDER ─────────────────────────────────────
logger.info("Running OPTIMIZE + ZORDER on %s ...", full_output_tbl)

spark.sql(f"""
    OPTIMIZE {full_output_tbl}
    WHERE month_start_date = DATE '{month_start_date_str}'
    ZORDER BY (content_id)
""")

logger.info("OPTIMIZE complete. ✓")

# COMMAND ----------
# ── 7. POST-RUN STATS ────────────────────────────────────────
stats_df = spark.sql(f"""
    SELECT
        month_start_date,
        COUNT(*)                          AS row_count,
        SUM(total_watch_time_minutes)     AS total_platform_watch_time_minutes,
        SUM(unique_viewers)               AS total_unique_viewers,
        MIN(watch_time_dense_rank)        AS min_dense_rank,
        MAX(watch_time_dense_rank)        AS max_dense_rank
    FROM {full_output_tbl}
    WHERE month_start_date = DATE '{month_start_date_str}'
    GROUP BY month_start_date
""")

logger.info("Post-run statistics:")
stats_df.show(truncate=False)

# Surface as notebook output for Airflow / Databricks Jobs UI
dbutils.notebook.exit("SUCCESS")
