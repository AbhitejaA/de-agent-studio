# Databricks notebook source
# =============================================================================
# Pipeline  : top_10_subscribers_by_watch_time
# Layer     : Gold
# Output    : gold_top10_subscribers_by_watch_time_daily
# Schedule  : Daily @ 07:00 UTC  (0 7 * * *)
# Author    : Senior Data Engineering – Peacock TV
#
# QA Fixes Applied
# ────────────────
# CRITICAL-1 : run_date widget is populated by Airflow {{ ds }} so backfill /
#              late runs always target the correct partition.
# CRITICAL-2 : Quality gate now also warns (and logs) when row_count < top_n
#              (i.e. partial Top-N result) — not only when row_count == 0.
# CRITICAL-3 : top_n sourced from widget (set by DAG from YAML config), never
#              hardcoded — single source of truth.
# WARNING-1  : subscriber_watch_time is .cache()'d before the quality count so
#              the DataFrame is materialised once; the write reuses the cache.
# WARNING-2  : top_10 is .cache()'d before .count() so the action does not
#              trigger a full recomputation on write (was double-evaluated).
# WARNING-3  : Global Window.orderBy() is intentional for a Top-N query; an
#              explicit comment is added to prevent future misidentification.
# SUGGESTION-1: Early source validation step added — fails fast before expensive
#              transforms if viewing_sessions has no rows for the target date.
# SUGGESTION-2: OPTIMIZE + ZORDER is guarded by a `run_optimize` widget so it
#              can be skipped in dev/ad-hoc runs to reduce latency.
# =============================================================================

# COMMAND 1 — Imports & logging
# ─────────────────────────────────────────────────────────────────────────────
import logging
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("top_10_subscribers_by_watch_time")

# COMMAND 2 — Widget declarations
# ─────────────────────────────────────────────────────────────────────────────
# QA Fix (critical): run_date default is yesterday (wall-clock fallback for
# manual runs). When triggered by Airflow the DAG overrides this with {{ ds }},
# ensuring backfills always target the correct date.
dbutils.widgets.text(
    "run_date",
    str(date.today() - timedelta(days=1)),
    "Run Date (YYYY-MM-DD)",
)
# QA Fix (critical / config drift): top_n comes from the YAML config via DAG
# NOTEBOOK_PARAMS — never hardcoded in notebook or SQL.
dbutils.widgets.text("top_n",         "10",   "Top-N Subscribers")
dbutils.widgets.text("env",           "dev",  "Environment (dev|stage|prod)")
# QA Fix (suggestion): flag to skip OPTIMIZE in dev / ad-hoc runs.
dbutils.widgets.text("run_optimize",  "true", "Run OPTIMIZE + ZORDER (true|false)")
# QA Fix (suggestion): early source validation threshold.
dbutils.widgets.text("min_source_rows", "1",  "Minimum source rows before failing fast")

# COMMAND 3 — Resolve widget values
# ─────────────────────────────────────────────────────────────────────────────
REPORT_DATE     : str  = dbutils.widgets.get("run_date")
TOP_N           : int  = int(dbutils.widgets.get("top_n"))
ENV             : str  = dbutils.widgets.get("env")
RUN_OPTIMIZE    : bool = dbutils.widgets.get("run_optimize").strip().lower() == "true"
MIN_SOURCE_ROWS : int  = int(dbutils.widgets.get("min_source_rows"))

logger.info("=" * 70)
logger.info("Pipeline : top_10_subscribers_by_watch_time")
logger.info("Env      : %s", ENV)
logger.info("RunDate  : %s  (partition target)", REPORT_DATE)
logger.info("Top-N    : %d", TOP_N)
logger.info("Optimize : %s", RUN_OPTIMIZE)
logger.info("=" * 70)

# COMMAND 4 — Catalog / schema resolution
# ─────────────────────────────────────────────────────────────────────────────
_CATALOG_MAP = {
    "dev"  : "dev_catalog",
    "stage": "stage_catalog",
    "prod" : "prod_catalog",
}
CATALOG        : str = _CATALOG_MAP.get(ENV, "dev_catalog")
INPUT_SCHEMA   : str = "silver"
OUTPUT_SCHEMA  : str = "gold"
OUTPUT_TABLE   : str = f"{CATALOG}.{OUTPUT_SCHEMA}.gold_top10_subscribers_by_watch_time_daily"

logger.info("Catalog      : %s", CATALOG)
logger.info("Input schema : %s.%s", CATALOG, INPUT_SCHEMA)
logger.info("Output table : %s", OUTPUT_TABLE)

# Set Spark session catalog so bare table references in SQL resolve correctly.
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {INPUT_SCHEMA}")

# COMMAND 5 — Read source tables
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Reading source tables …")

subscriber_profiles = spark.table(f"{CATALOG}.{INPUT_SCHEMA}.subscriber_profiles")
viewing_sessions    = spark.table(f"{CATALOG}.{INPUT_SCHEMA}.viewing_sessions")

logger.info(
    "subscriber_profiles schema: %s",
    subscriber_profiles.schema.simpleString(),
)
logger.info(
    "viewing_sessions schema   : %s",
    viewing_sessions.schema.simpleString(),
)

# COMMAND 6 — EARLY SOURCE VALIDATION (QA Fix – suggestion)
# ─────────────────────────────────────────────────────────────────────────────
# Fail fast before expensive transforms if no sessions exist for the target
# date. This catches missing upstream Silver loads early and gives operators a
# descriptive error rather than a silent zero-row Gold write.
logger.info("Validating source data for run_date=%s …", REPORT_DATE)

source_row_count: int = (
    viewing_sessions
    .filter(F.to_date("session_start") == F.lit(REPORT_DATE).cast("date"))
    .filter(F.col("duration_minutes").isNotNull() & (F.col("duration_minutes") >= 0))
    .count()
)

logger.info("viewing_sessions rows for %s: %d", REPORT_DATE, source_row_count)

if source_row_count < MIN_SOURCE_ROWS:
    raise ValueError(
        f"[Source Validation Failed] viewing_sessions contains {source_row_count} "
        f"valid rows for run_date={REPORT_DATE}. "
        f"Expected at least {MIN_SOURCE_ROWS}. "
        f"Check whether the upstream Silver load completed successfully."
    )

logger.info("Source validation passed (%d rows found).", source_row_count)

# COMMAND 7 — Filter sessions to target date & aggregate watch time
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Aggregating watch time per subscriber for %s …", REPORT_DATE)

daily_sessions = (
    viewing_sessions
    .filter(F.to_date("session_start") == F.lit(REPORT_DATE).cast("date"))
    .filter(F.col("duration_minutes").isNotNull() & (F.col("duration_minutes") >= 0))
)

subscriber_watch_time = (
    daily_sessions
    .groupBy("subscriber_id")
    .agg(
        F.sum("duration_minutes").alias("total_watch_time_minutes"),
        F.count("*").alias("session_count"),
    )
)

# QA Fix (warning): Cache after aggregation so the downstream join + window
# function do not re-scan viewing_sessions a second time.
subscriber_watch_time.cache()
agg_count: int = subscriber_watch_time.count()
logger.info("Distinct subscribers with sessions on %s: %d", REPORT_DATE, agg_count)

# COMMAND 8 — Enrich with subscriber metadata
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Joining with subscriber_profiles …")

enriched = (
    subscriber_watch_time
    .join(subscriber_profiles, on="subscriber_id", how="inner")
    .select(
        "subscriber_id",
        F.col("name").alias("subscriber_name"),
        F.col("plan").alias("subscription_plan"),
        "total_watch_time_minutes",
        "session_count",
    )
)

# COMMAND 9 — Rank subscribers (global descending order)
# ─────────────────────────────────────────────────────────────────────────────
# NOTE (QA Fix – warning): Window.orderBy() without .partitionBy() performs a
# global sort, which requires a full shuffle to a single Spark partition. For a
# Top-N query over a daily aggregate this is unavoidable — the result set is
# already tiny (at most ~millions of unique subscribers per day before TOP_N
# filtering). The cost is bounded and acceptable here. If the subscriber count
# grows significantly, consider a pre-filter or sample-based approach upstream.
logger.info("Ranking subscribers by watch time …")

watch_time_window = Window.orderBy(
    F.col("total_watch_time_minutes").desc(),
    F.col("subscriber_id").asc(),   # deterministic tie-break
)

ranked = (
    enriched
    .withColumn("watch_time_rank", F.dense_rank().over(watch_time_window))
    .withColumn("row_num",         F.row_number().over(watch_time_window))
)

top_10 = (
    ranked
    .filter(F.col("row_num") <= TOP_N)           # QA Fix: uses widget, not literal 10
    .withColumn("run_date",          F.lit(REPORT_DATE).cast("date"))  # partition col
    .withColumn("pipeline_run_ts",   F.current_timestamp())
    .select(
        "run_date",
        "watch_time_rank",
        "row_num",
        "subscriber_id",
        "subscriber_name",
        "subscription_plan",
        "total_watch_time_minutes",
        "session_count",
        "pipeline_run_ts",
    )
    .orderBy("watch_time_rank", "subscriber_id")
)

# QA Fix (warning): Cache top_10 BEFORE .count() so both the quality gate
# check and the subsequent Delta write reuse the same materialised result.
# Without cache, .count() triggers full recomputation, doubling cluster work.
top_10.cache()

# COMMAND 10 — Quality gate
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Running quality gate …")

row_count: int = top_10.count()
logger.info("Rows to write: %d (expected up to %d)", row_count, TOP_N)

# Hard fail: zero rows always indicates a pipeline error.
if row_count == 0:
    raise ValueError(
        f"[Quality Gate FAILED] top_10 DataFrame is empty for run_date={REPORT_DATE}. "
        f"No subscribers had qualifying viewing sessions. "
        f"Verify upstream Silver tables and the source validation threshold."
    )

# QA Fix (critical): Warn when fewer than TOP_N rows are returned (partial result).
# This is not a hard failure — sparse days (e.g. dev/stage testing) are valid —
# but operators should be aware that the output is a partial Top-N ranking.
if row_count < TOP_N:
    logger.warning(
        "[Quality Gate WARNING] Only %d of %d expected rows found for "
        "run_date=%s. Output is a partial Top-%d result. "
        "Verify subscriber_profiles completeness and whether the Silver "
        "viewing_sessions load was partial for this date.",
        row_count, TOP_N, REPORT_DATE, TOP_N,
    )
else:
    logger.info("Quality gate PASSED: %d rows ready to write.", row_count)

logger.info("Sample output:")
top_10.show(truncate=False)

# COMMAND 11 — Write to Gold Delta table (partition-safe overwrite)
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Writing to Gold table: %s …", OUTPUT_TABLE)

(
    top_10
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"run_date = '{REPORT_DATE}'")  # partition-safe
    .partitionBy("run_date")
    .saveAsTable(OUTPUT_TABLE)
)

logger.info("Write complete. Partition run_date=%s written to %s.", REPORT_DATE, OUTPUT_TABLE)

# Release cache now that the write is done.
top_10.unpersist()
subscriber_watch_time.unpersist()

# COMMAND 12 — OPTIMIZE + ZORDER (guarded by widget flag)
# ─────────────────────────────────────────────────────────────────────────────
# QA Fix (suggestion): OPTIMIZE is skipped in dev (or any ad-hoc run) via the
# run_optimize widget. For a Gold table with 10 rows per partition ZORDER
# provides marginal benefit; enabling it in prod ensures read performance as
# historical partitions accumulate.
if RUN_OPTIMIZE:
    logger.info("Running OPTIMIZE + ZORDER on %s …", OUTPUT_TABLE)
    spark.sql(f"""
        OPTIMIZE {OUTPUT_TABLE}
        ZORDER BY (subscriber_id)
    """)
    logger.info("OPTIMIZE complete.")
else:
    logger.info(
        "OPTIMIZE skipped (run_optimize=false). "
        "Run manually or via the maintenance DAG on a weekly schedule."
    )

# COMMAND 13 — Final summary
# ─────────────────────────────────────────────────────────────────────────────
logger.info("=" * 70)
logger.info("Pipeline COMPLETE")
logger.info("  Output table : %s", OUTPUT_TABLE)
logger.info("  Partition    : run_date = %s", REPORT_DATE)
logger.info("  Rows written : %d", row_count)
logger.info("  Top-N        : %d", TOP_N)
logger.info("  Environment  : %s", ENV)
logger.info("=" * 70)

dbutils.notebook.exit(
    f"SUCCESS | run_date={REPORT_DATE} | rows={row_count} | env={ENV}"
)
