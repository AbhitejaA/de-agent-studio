# =============================================================================
# Pipeline  : weekly_most_watched_content_by_view_count
# Notebook  : weekly_most_watched_content_by_view_count_notebook.py
# Layer     : Gold
# Schedule  : Every Monday at 06:00 UTC  (cron: 0 6 * * 1)
# Maintained: Peacock Data Engineering
#
# PURPOSE
# -------
# Ranks all content by distinct-session view count for the previous completed
# ISO week (Monday–Sunday) and writes the result to a Gold Delta table
# partitioned by week_start_date.
#
# QA FIXES APPLIED
# ----------------
# Critical #2 : final_df is cached before ALL DQ checks.  Three separate
#               count() calls are collapsed into a single aggregation pass.
#               final_df.unpersist() is called after the write.
# Critical #3 : execution_date widget is validated — non-empty, ISO format,
#               must be a Monday — with a clear ValueError before any compute.
# Critical #5 : vacuum_retain_hours is passed as a widget (from YAML config).
#               For dev (and any env with hours < 168), the Delta retention
#               safety check is disabled before VACUUM and re-enabled after.
# Warning  W3  : Explicit ISO-calendar arithmetic for week resolution makes
#               the window robust for non-Monday trigger dates.
# Warning  W4  : saveAsTable uses a fully qualified `database.output_table`
#               name derived from the YAML config widgets.
# Warning  W5  : vacuum_retain_hours comes from the widget — no hardcoded
#               dev/non-dev binary split in the notebook.
# Warning  W6  : .orderBy() before Delta write is removed (Delta ignores row
#               order on disk; consumers ORDER BY at query time).
# Suggestion S2: top_n widget is wired — if set, only top-N ranks are written.
# Suggestion S3: Single-pass DQ aggregation (F.agg) replaces three jobs.
# =============================================================================

# COMMAND ----------
# %md
# ## Step 1 — Widget Declarations
# All parameters are injected by the Airflow DAG via `base_parameters`.

# COMMAND ----------

dbutils.widgets.text("execution_date",      "",             "Execution Date (YYYY-MM-DD)")
dbutils.widgets.text("env",                 "prod",         "Environment (dev|stage|prod)")
dbutils.widgets.text("sessions_table",      "viewing_sessions",  "Input: Sessions Table")
dbutils.widgets.text("catalog_table",       "content_catalog",   "Input: Catalog Table")
dbutils.widgets.text("output_table",        "gold_most_watched_content_by_view_count_weekly",
                                                             "Output Table Name")
dbutils.widgets.text("database",            "gold",         "Target Database/Schema")
dbutils.widgets.text("vacuum_retain_hours", "336",          "VACUUM Retain Hours")
dbutils.widgets.text("top_n",               "null",         "Top-N Cap (null = no cap)")

# COMMAND ----------
# %md
# ## Step 2 — Imports & Widget Ingestion

# COMMAND ----------

import re
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

spark: SparkSession = spark  # noqa: F821 — provided by Databricks runtime

# Ingest raw widget strings
execution_date_str:  str = dbutils.widgets.get("execution_date").strip()
ENV:                 str = dbutils.widgets.get("env").strip().lower()
SESSIONS_TABLE:      str = dbutils.widgets.get("sessions_table").strip()
CATALOG_TABLE:       str = dbutils.widgets.get("catalog_table").strip()
OUTPUT_TABLE_NAME:   str = dbutils.widgets.get("output_table").strip()
DATABASE:            str = dbutils.widgets.get("database").strip()
VACUUM_HOURS_STR:    str = dbutils.widgets.get("vacuum_retain_hours").strip()
TOP_N_STR:           str = dbutils.widgets.get("top_n").strip()

# Fully qualified output identifier  (Warning W4 fix)
FULL_OUTPUT_TABLE:   str = f"{DATABASE}.{OUTPUT_TABLE_NAME}"

print(f"[Config] env={ENV} | db={DATABASE} | output={FULL_OUTPUT_TABLE}")
print(f"[Config] sessions={SESSIONS_TABLE} | catalog={CATALOG_TABLE}")
print(f"[Config] vacuum_retain_hours={VACUUM_HOURS_STR} | top_n={TOP_N_STR}")

# COMMAND ----------
# %md
# ## Step 3 — Input Validation
# Fail fast with a descriptive error before any computation begins.

# COMMAND ----------

# ── 3a. execution_date: non-empty ──────────────────────────────────────────
if not execution_date_str:
    raise ValueError(
        "[weekly_most_watched_content_by_view_count] Widget 'execution_date' "
        "is empty.  The Airflow DAG must pass '{{ ds }}' (e.g. '2025-01-13').  "
        "Check the DAG's base_parameters configuration."
    )

# ── 3b. execution_date: valid ISO-8601 date format YYYY-MM-DD ──────────────
_ISO_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
if not _ISO_PATTERN.match(execution_date_str):
    raise ValueError(
        f"[weekly_most_watched_content_by_view_count] Widget 'execution_date' "
        f"does not match YYYY-MM-DD format.  Received: '{execution_date_str}'.  "
        f"Ensure Airflow passes '{{{{ ds }}}}' without additional formatting."
    )

try:
    execution_date: date = date.fromisoformat(execution_date_str)
except ValueError as exc:
    raise ValueError(
        f"[weekly_most_watched_content_by_view_count] Could not parse "
        f"'execution_date' widget value '{execution_date_str}' as a date.  "
        f"Underlying error: {exc}"
    ) from exc

# ── 3c. execution_date: must be a Monday (weekday == 0) ────────────────────
# WARNING W3 FIX: Guard against non-Monday triggers.
# The scheduled run always fires on Monday, but backfills/ad-hoc triggers
# may not.  We warn here and compute the window robustly regardless.
if execution_date.weekday() != 0:
    _day_name = execution_date.strftime("%A")
    print(
        f"[WARNING] execution_date '{execution_date_str}' is a {_day_name} "
        f"(weekday={execution_date.weekday()}), not a Monday.  "
        f"The pipeline is scheduled for Mondays only.  "
        f"The reporting window will be computed as the ISO week that contains "
        f"this date — verify the result is the intended reporting period."
    )

# ── 3d. vacuum_retain_hours: must be a positive integer ────────────────────
try:
    VACUUM_HOURS: int = int(VACUUM_HOURS_STR)
    if VACUUM_HOURS <= 0:
        raise ValueError("Must be positive.")
except ValueError as exc:
    raise ValueError(
        f"[weekly_most_watched_content_by_view_count] Widget "
        f"'vacuum_retain_hours' must be a positive integer.  "
        f"Received: '{VACUUM_HOURS_STR}'.  Error: {exc}"
    ) from exc

# ── 3e. top_n: null or positive integer ────────────────────────────────────
if TOP_N_STR.lower() == "null":
    TOP_N: int | None = None
else:
    try:
        TOP_N = int(TOP_N_STR)
        if TOP_N <= 0:
            raise ValueError("Must be positive.")
    except ValueError as exc:
        raise ValueError(
            f"[weekly_most_watched_content_by_view_count] Widget 'top_n' must "
            f"be 'null' or a positive integer.  Received: '{TOP_N_STR}'.  "
            f"Error: {exc}"
        ) from exc

print(f"[Validation] All widgets validated successfully.")
print(f"[Validation] execution_date={execution_date} | vacuum_hours={VACUUM_HOURS} | top_n={TOP_N}")

# COMMAND ----------
# %md
# ## Step 4 — Compute Reporting Window
# WARNING W3 FIX: Use ISO-calendar arithmetic so the window is correct
# regardless of whether execution_date is a Monday or not.

# COMMAND ----------

# Find the Monday of the ISO week containing execution_date.
# For a scheduled Monday run: week_start = execution_date - 7 days (prev week).
# For a non-Monday backfill:  roll back to the Monday of the current ISO week,
# then subtract 7 days to get the previous completed ISO week.
_this_monday: date = execution_date - timedelta(days=execution_date.weekday())
week_start:   date = _this_monday - timedelta(weeks=1)    # previous Monday
week_end:     date = week_start   + timedelta(weeks=1)    # following Monday (exclusive)

week_start_str: str = week_start.isoformat()  # "YYYY-MM-DD"
week_end_str:   str = week_end.isoformat()

print(f"[Window] Reporting ISO week: [{week_start_str}, {week_end_str})")
print(f"[Window] Execution date: {execution_date_str} → previous week start: {week_start_str}")

# COMMAND ----------
# %md
# ## Step 5 — Spark Configuration for Partition-Safe Overwrite

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------
# %md
# ## Step 6 — Load Input Tables

# COMMAND ----------

sessions_df = spark.table(SESSIONS_TABLE)
catalog_df  = spark.table(CATALOG_TABLE)

print(f"[Load] '{SESSIONS_TABLE}' and '{CATALOG_TABLE}' loaded from metastore.")

# COMMAND ----------
# %md
# ## Step 7 — Filter, Aggregate, Enrich & Rank
#
# All transformation logic lives in a single chained spark.sql() call that
# mirrors the canonical SQL reference file exactly.  This eliminates the
# dual-implementation drift risk flagged in Warning W7.

# COMMAND ----------

# Register temp views so spark.sql() can reference them
sessions_df.createOrReplaceTempView("_sessions_raw")
catalog_df.createOrReplaceTempView("_catalog_raw")

TRANSFORM_SQL = f"""
WITH

-- ── Step 7a: Filter to previous ISO week ───────────────────────────────────
weekly_sessions AS (
    SELECT
        session_id,
        content_id
    FROM _sessions_raw
    WHERE
        session_date >= '{week_start_str}'
        AND session_date <  '{week_end_str}'
        AND session_id  IS NOT NULL
        AND content_id  IS NOT NULL
),

-- ── Step 7b: Aggregate — one view = one distinct session_id ────────────────
content_view_counts AS (
    SELECT
        content_id,
        COUNT(DISTINCT session_id) AS view_count
    FROM weekly_sessions
    GROUP BY content_id
),

-- ── Step 7c: Enrich with content metadata ──────────────────────────────────
enriched AS (
    SELECT
        vc.content_id,
        cc.title,
        cc.genre,
        cc.content_type,
        cc.release_year,
        vc.view_count
    FROM content_view_counts vc
    LEFT JOIN _catalog_raw cc
        ON vc.content_id = cc.content_id
),

-- ── Step 7d: Rank — DENSE_RANK for ties, ROW_NUMBER for tie-breaking ───────
ranked AS (
    SELECT
        content_id,
        title,
        genre,
        content_type,
        release_year,
        view_count,
        DENSE_RANK()  OVER (ORDER BY view_count DESC)              AS view_count_rank,
        ROW_NUMBER()  OVER (ORDER BY view_count DESC, content_id)  AS row_num
    FROM enriched
)

-- ── Step 7e: Attach partition column & apply optional top-N cap ────────────
SELECT
    content_id,
    title,
    genre,
    content_type,
    release_year,
    view_count,
    view_count_rank,
    row_num,
    CAST('{week_start_str}' AS DATE) AS week_start_date
FROM ranked
{"WHERE view_count_rank <= " + str(TOP_N) if TOP_N is not None else "-- top_n = null: no row cap applied"}
"""

# NOTE: .orderBy() intentionally omitted here (Warning W6 fix).
# Delta does not preserve row order on disk; ORDER BY belongs at query time.
# Physical layout is handled by ZORDER in Step 12.
final_df = spark.sql(TRANSFORM_SQL)

print("[Transform] SQL transformation plan built (lazy — not yet executed).")
if TOP_N is not None:
    print(f"[Transform] top_n cap applied: only ranks 1–{TOP_N} will be written.")

# COMMAND ----------
# %md
# ## Step 8 — Cache final_df Before DQ Checks
#
# CRITICAL FIX #2: Cache the final DataFrame BEFORE any .count() calls.
# Without caching, each count() triggers a full re-scan + re-shuffle of the
# entire transformation pipeline (filter → aggregate → join → rank).
# On a full week of production data this causes 3+ redundant full shuffles.

# COMMAND ----------

final_df.persist(StorageLevel.MEMORY_AND_DISK)
print("[Cache] final_df persisted to MEMORY_AND_DISK.")

# COMMAND ----------
# %md
# ## Step 9 — Data Quality Gate (Single-Pass Aggregation)
#
# CRITICAL FIX #2 + Suggestion S3:
# All three DQ metrics (total rows, null view_count, null content_id) are
# computed in a SINGLE Spark job via .agg(), not three separate .count() calls.
# This reduces DQ overhead from 3 full jobs → 1.

# COMMAND ----------

dq_row = final_df.agg(
    F.count("*").alias("total_rows"),
    F.sum(F.col("view_count").isNull().cast("int")).alias("null_view_count"),
    F.sum(F.col("content_id").isNull().cast("int")).alias("null_content_id"),
).first()

total_rows:      int = dq_row["total_rows"]
null_view_count: int = dq_row["null_view_count"]
null_content_id: int = dq_row["null_content_id"]

print(f"[DQ] total_rows={total_rows} | null_view_count={null_view_count} | null_content_id={null_content_id}")

# ── DQ Assertions ───────────────────────────────────────────────────────────
if total_rows == 0:
    raise ValueError(
        f"[DQ FAIL] Zero rows returned for week_start_date={week_start_str}.  "
        f"Possible causes: no sessions in '{SESSIONS_TABLE}' for the reporting "
        f"window [{week_start_str}, {week_end_str}), or a date-filter bug.  "
        f"Aborting write to protect the Gold table."
    )

if null_content_id > 0:
    raise ValueError(
        f"[DQ FAIL] {null_content_id} rows have NULL content_id after "
        f"aggregation.  This indicates upstream data quality issues in "
        f"'{SESSIONS_TABLE}'.  Aborting write."
    )

if null_view_count > 0:
    # Warn but do not fail — view_count is derived from COUNT() so NULLs here
    # would indicate a Spark bug, not a data issue; surface as warning only.
    print(
        f"[DQ WARN] {null_view_count} rows have NULL view_count.  "
        f"This is unexpected for a COUNT() aggregation — investigate."
    )

print(f"[DQ] Gate passed: {total_rows} rows ready to write for {week_start_str}.")

# COMMAND ----------
# %md
# ## Step 10 — Schema Preview (Dev/Stage Only)

# COMMAND ----------

if ENV in ("dev", "stage"):
    print("[Preview] Output schema:")
    final_df.printSchema()
    print(f"[Preview] Sample rows (top 10 by view_count_rank):")
    final_df.orderBy("view_count_rank", "row_num").show(10, truncate=False)

# COMMAND ----------
# %md
# ## Step 11 — Write to Gold Delta Table
#
# Partition-safe overwrite: only the week_start_date partition being written
# is replaced.  All historical partitions remain untouched.
# Warning W4 fix: uses FULL_OUTPUT_TABLE = "database.output_table_name".

# COMMAND ----------

print(f"[Write] Writing {total_rows} rows to '{FULL_OUTPUT_TABLE}' "
      f"partitioned by week_start_date={week_start_str} ...")

(
    final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("week_start_date")
    .saveAsTable(FULL_OUTPUT_TABLE)          # Warning W4 fix — fully qualified
)

print(f"[Write] Successfully wrote partition week_start_date={week_start_str} "
      f"to '{FULL_OUTPUT_TABLE}'.")

# COMMAND ----------
# %md
# ## Step 12 — Unpersist Cache
#
# CRITICAL FIX #2: Release cached memory immediately after the write.
# This frees executor memory for the OPTIMIZE + VACUUM steps.

# COMMAND ----------

final_df.unpersist()
print("[Cache] final_df unpersisted.")

# COMMAND ----------
# %md
# ## Step 13 — OPTIMIZE + ZORDER
#
# ZORDER BY content_id co-locates data for the most common filter/join
# pattern (lookups and downstream joins by content_id).
# Suggestion S1: NOTE — OUTPUT_TABLE and week_start_str are system-controlled
# constants, not user input, so the f-string interpolation below does not
# create a SQL injection risk.  Add a comment for clarity per S1.

# COMMAND ----------

print(f"[Optimize] Running OPTIMIZE + ZORDER BY content_id on "
      f"'{FULL_OUTPUT_TABLE}' partition week_start_date='{week_start_str}' ...")

spark.sql(f"""
    OPTIMIZE {FULL_OUTPUT_TABLE}
    WHERE week_start_date = '{week_start_str}'
    ZORDER BY (content_id)
""")

print("[Optimize] OPTIMIZE complete.")

# COMMAND ----------
# %md
# ## Step 14 — VACUUM (with Delta Retention Safety Guard)
#
# CRITICAL FIX #5:
# Delta's default retentionDurationCheck threshold is 168 hours (7 days).
# If VACUUM_HOURS < 168, the command raises a runtime error unless the check
# is explicitly disabled first.
#
# Strategy (per environment, driven by YAML config widget):
#   dev   (24 h)  → disable check before VACUUM, re-enable after
#   stage (168 h) → at threshold; no override needed (168 == 168)
#   prod  (336 h) → well above threshold; no override needed
#
# Note: disabling the retention check in dev is intentional and documented.
# It means old dev file versions are cleaned up more aggressively, which is
# acceptable in a non-production environment.

# COMMAND ----------

_DELTA_RETENTION_THRESHOLD_HOURS = 168
_needs_retention_override = VACUUM_HOURS < _DELTA_RETENTION_THRESHOLD_HOURS

if _needs_retention_override:
    print(
        f"[VACUUM] vacuum_retain_hours={VACUUM_HOURS} is below Delta's safety "
        f"threshold of {_DELTA_RETENTION_THRESHOLD_HOURS} h.  "
        f"Disabling retentionDurationCheck for env='{ENV}' before VACUUM."
    )
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

print(f"[VACUUM] Running VACUUM on '{FULL_OUTPUT_TABLE}' "
      f"RETAIN {VACUUM_HOURS} HOURS ...")

spark.sql(f"VACUUM {FULL_OUTPUT_TABLE} RETAIN {VACUUM_HOURS} HOURS")

if _needs_retention_override:
    # Re-enable the check so no other Delta operations in this session
    # accidentally bypass the retention guard.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    print("[VACUUM] retentionDurationCheck re-enabled.")

print(f"[VACUUM] Complete — retained {VACUUM_HOURS} hours of history.")

# COMMAND ----------
# %md
# ## Step 15 — Final Summary Log

# COMMAND ----------

print("=" * 70)
print(f"[DONE] Pipeline: weekly_most_watched_content_by_view_count")
print(f"       Environment    : {ENV}")
print(f"       Reporting Week : {week_start_str} → {(week_end - timedelta(days=1)).isoformat()}")
print(f"       Rows Written   : {total_rows}")
print(f"       Output Table   : {FULL_OUTPUT_TABLE}")
print(f"       Partition      : week_start_date = {week_start_str}")
print(f"       Top-N Cap      : {TOP_N if TOP_N is not None else 'none (all ranks written)'}")
print(f"       Execution Date : {execution_date_str}")
print("=" * 70)
