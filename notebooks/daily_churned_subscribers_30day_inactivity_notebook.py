# Databricks notebook source
# =============================================================================
# Notebook : daily_churned_subscribers_30day_inactivity_notebook
# Layer    : Gold
# Output   : {catalog}.{schema}.gold_churned_subscribers_30day_inactivity
# Schedule : 0 6 * * *  (06:00 UTC daily via Airflow DAG)
# Author   : Peacock Data Engineering
#
# QA FIXES APPLIED:
#   FIX-1  : RUN_DATE strictly validated with datetime.strptime() in Step 0
#             immediately after widget retrieval — raises ValueError on bad input
#             before any SQL is executed, preventing SQL injection via f-strings.
#   FIX-2  : result DataFrame cached with result.cache() before first .count()
#             and unpersisted after write — transformation executes exactly once.
#   FIX-3  : Expensive input-table row-count scans removed from prod/stage paths
#             and gated behind ENV == 'dev' flag only.
#   FIX-4  : All SQL uses fully-qualified {catalog}.{schema}.table_name — no
#             unqualified table references anywhere in the notebook.
# =============================================================================

# COMMAND ----------
# =============================================================================
# Step 0 — Imports, widget setup, and STRICT parameter validation (FIX-1)
# =============================================================================

import logging
import re
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("daily_churned_subscribers_30day_inactivity")

# --- Widget declarations (base_parameters injected by Airflow DAG) -----------
dbutils.widgets.text("run_date", "")   # Expected: ISO-8601 YYYY-MM-DD
dbutils.widgets.text("env",      "dev")
dbutils.widgets.text("catalog",  "dev_catalog")
dbutils.widgets.text("schema",   "peacock_core")

RUN_DATE_RAW = dbutils.widgets.get("run_date").strip()
ENV          = dbutils.widgets.get("env").strip()
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()

# ---------------------------------------------------------------------------
# FIX-1 : Strict RUN_DATE validation — must pass BEFORE any SQL execution.
#          datetime.strptime enforces exact YYYY-MM-DD format.
#          A malformed or injected value raises ValueError immediately here.
# ---------------------------------------------------------------------------
try:
    _validated_dt = datetime.strptime(RUN_DATE_RAW, "%Y-%m-%d")
    RUN_DATE      = _validated_dt.strftime("%Y-%m-%d")   # canonical, safe string
    logger.info(f"[VALIDATED] run_date = {RUN_DATE}")
except ValueError as exc:
    raise ValueError(
        f"[ABORT] Invalid run_date widget value: '{RUN_DATE_RAW}'. "
        f"Expected ISO-8601 format YYYY-MM-DD. "
        f"Airflow should pass {{ ds }} which is always valid. Error: {exc}"
    )

# Additional guard: reject anything that looks like SQL injection even after parse
_SAFE_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
if not _SAFE_DATE_RE.match(RUN_DATE):
    raise ValueError(f"[ABORT] run_date failed safety regex after strptime: '{RUN_DATE}'")

# Validate ENV / CATALOG / SCHEMA — allow only alphanumeric + underscore
_SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9_]+$")
for _param_name, _param_val in [("env", ENV), ("catalog", CATALOG), ("schema", SCHEMA)]:
    if not _SAFE_ID_RE.match(_param_val):
        raise ValueError(
            f"[ABORT] Parameter '{_param_name}' contains invalid characters: '{_param_val}'. "
            f"Only alphanumeric and underscores are permitted."
        )

# ---------------------------------------------------------------------------
# Derived constants — built from validated strings only
# ---------------------------------------------------------------------------
INPUT_SUBSCRIBERS = f"{CATALOG}.{SCHEMA}.subscriber_profiles"
INPUT_SESSIONS    = f"{CATALOG}.{SCHEMA}.viewing_sessions"
OUTPUT_TABLE      = f"{CATALOG}.{SCHEMA}.gold_churned_subscribers_30day_inactivity"

logger.info(f"[CONFIG] env={ENV} | catalog={CATALOG} | schema={SCHEMA}")
logger.info(f"[CONFIG] run_date={RUN_DATE}")
logger.info(f"[CONFIG] input_subscribers={INPUT_SUBSCRIBERS}")
logger.info(f"[CONFIG] input_sessions={INPUT_SESSIONS}")
logger.info(f"[CONFIG] output_table={OUTPUT_TABLE}")

# COMMAND ----------
# =============================================================================
# Step 1 — Configure Spark session
# =============================================================================

spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",   "true")
spark.conf.set("spark.sql.adaptive.enabled",                   "true")

logger.info("[SPARK] Session configured.")

# COMMAND ----------
# =============================================================================
# Step 2 — Set active catalog and schema for the session
# =============================================================================

# Use validated CATALOG/SCHEMA — safe because they passed _SAFE_ID_RE above
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
logger.info(f"[CATALOG] Active context set to {CATALOG}.{SCHEMA}")

# COMMAND ----------
# =============================================================================
# Step 3 — Load input tables
#           FIX-3: Input row-count scans gated behind ENV == 'dev' ONLY.
#                  Removed from stage/prod to avoid full-table scans on
#                  billions of rows (e.g. viewing_sessions in production).
# =============================================================================

subscriber_profiles = spark.table(INPUT_SUBSCRIBERS)
viewing_sessions    = spark.table(INPUT_SESSIONS)

logger.info(f"[LOAD] subscriber_profiles loaded from {INPUT_SUBSCRIBERS}")
logger.info(f"[LOAD] viewing_sessions    loaded from {INPUT_SESSIONS}")

# FIX-3: Only run expensive full-scan counts in dev (for debugging / data-quality checks)
if ENV == "dev":
    sp_count = subscriber_profiles.count()
    vs_count = viewing_sessions.count()
    logger.debug(f"[DEV ONLY] subscriber_profiles row count : {sp_count:,}")
    logger.debug(f"[DEV ONLY] viewing_sessions    row count : {vs_count:,}")
else:
    logger.info("[SKIP] Input row-count scans skipped in stage/prod to avoid full table scans.")

# COMMAND ----------
# =============================================================================
# Step 4 — Filter to churned subscribers (is_active = FALSE)
# =============================================================================

churned = subscriber_profiles.filter(F.col("is_active") == False)
logger.info("[FILTER] Applied: is_active = FALSE")

# COMMAND ----------
# =============================================================================
# Step 5 — Aggregate viewing_sessions to get last watch date per subscriber
# =============================================================================

last_watch = (
    viewing_sessions
    .groupBy("subscriber_id")
    .agg(F.max("session_start_date").alias("last_watch_date"))
)
logger.info("[AGG] last_watch_date calculated per subscriber_id")

# COMMAND ----------
# =============================================================================
# Step 6 — JOIN churned subscribers with last watch activity
# =============================================================================

joined = churned.join(last_watch, on="subscriber_id", how="left")
logger.info("[JOIN] LEFT JOIN applied: churned subscribers ← last watch activity")

# COMMAND ----------
# =============================================================================
# Step 7 — Apply 30-day inactivity filter and project output columns
#           FIX-2: Cache result BEFORE first .count() so the transformation
#                  executes exactly once (not once per .count() + once per write).
# =============================================================================

run_date_lit = F.lit(RUN_DATE).cast("date")   # built from validated string

result = (
    joined
    .withColumn("days_since_last_watch",
                F.datediff(run_date_lit, F.col("last_watch_date")))
    .filter(
        (F.col("days_since_last_watch") > 30) |
        F.col("last_watch_date").isNull()
    )
    .select(
        F.col("subscriber_id"),
        F.col("subscription_plan"),
        F.col("churn_date"),
        F.col("last_watch_date"),
        F.col("days_since_last_watch"),
        run_date_lit.alias("run_date"),
    )
)

# FIX-2: Persist result so the transformation runs exactly once across
#         the upcoming .count() and .write operations.
result.persist(StorageLevel.MEMORY_AND_DISK)
logger.info("[CACHE] result DataFrame persisted (MEMORY_AND_DISK)")

output_count = result.count()
logger.info(f"[TRANSFORM] Rows matching 30-day inactivity criteria: {output_count:,}")

if output_count == 0:
    logger.warning("[WARN] Zero churned/inactive subscribers found for run_date=%s. "
                   "Proceeding with empty write to preserve partition.", RUN_DATE)

# COMMAND ----------
# =============================================================================
# Step 8 — Write to Delta gold table, partitioned by run_date (overwrite)
# =============================================================================

(
    result
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"run_date = '{RUN_DATE}'")   # RUN_DATE validated in Step 0
    .partitionBy("run_date")
    .saveAsTable(OUTPUT_TABLE)
)
logger.info(f"[WRITE] Delta write complete → {OUTPUT_TABLE} | partition run_date={RUN_DATE}")

# FIX-2: Release cache immediately after write — free cluster memory
result.unpersist()
logger.info("[CACHE] result DataFrame unpersisted")

# COMMAND ----------
# =============================================================================
# Step 9 — OPTIMIZE the output table for the written partition
#           RUN_DATE is validated (strptime + regex) in Step 0 — safe to use here.
# =============================================================================

spark.sql(f"OPTIMIZE {OUTPUT_TABLE} WHERE run_date = '{RUN_DATE}'")
logger.info(f"[OPTIMIZE] OPTIMIZE complete on {OUTPUT_TABLE} WHERE run_date = '{RUN_DATE}'")

# COMMAND ----------
# =============================================================================
# Step 10 — Post-write validation
#            RUN_DATE is validated in Step 0 — safe for f-string SQL here.
#            Single count executed on the persisted Delta table (not on result DF).
# =============================================================================

validation_df = spark.sql(
    f"SELECT COUNT(*) AS row_count FROM {OUTPUT_TABLE} WHERE run_date = '{RUN_DATE}'"
)
validated_count = validation_df.collect()[0]["row_count"]
logger.info(f"[VALIDATE] Post-write row count in Delta table: {validated_count:,}")

if validated_count != output_count:
    raise RuntimeError(
        f"[DATA QUALITY FAILURE] Written row count mismatch! "
        f"Expected {output_count:,}, found {validated_count:,} in {OUTPUT_TABLE} "
        f"for run_date={RUN_DATE}."
    )

logger.info("[VALIDATE] Row count check PASSED ✓")

# COMMAND ----------
# =============================================================================
# Step 11 — Final summary
# =============================================================================

summary = {
    "pipeline":      "daily_churned_subscribers_30day_inactivity",
    "env":           ENV,
    "catalog":       CATALOG,
    "schema":        SCHEMA,
    "output_table":  OUTPUT_TABLE,
    "run_date":      RUN_DATE,
    "rows_written":  validated_count,
    "status":        "SUCCESS",
}
logger.info(f"[SUMMARY] {summary}")
print(summary)
