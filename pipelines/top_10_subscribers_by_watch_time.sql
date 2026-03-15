-- =============================================================================
-- Pipeline  : top_10_subscribers_by_watch_time
-- Layer     : Gold
-- Output    : gold_top10_subscribers_by_watch_time_daily
-- Schedule  : Daily @ 07:00 UTC  (0 7 * * *)
-- Author    : Senior Data Engineering – Peacock TV
-- Description:
--   Joins subscriber_profiles with viewing_sessions to calculate total watch
--   time (minutes) per subscriber for the PREVIOUS completed day, ranks
--   subscribers in descending order, and returns the top-N (default 10).
--
-- NOTE: This SQL is rendered and executed exclusively from the Databricks
--       notebook context (top_10_subscribers_by_watch_time_notebook.py).
--       The Databricks session catalog and input schema are pre-set by the
--       notebook before this SQL runs, so bare table names are intentional.
--       All ${param} placeholders are resolved via notebook widgets /
--       Airflow NOTEBOOK_PARAMS before execution.
--
-- QA Fix (warning): Replaced DATEADD(DAY, -1, CURRENT_DATE()) with the
--   portable ANSI / Spark SQL form: CURRENT_DATE() - INTERVAL 1 DAY.
-- QA Fix (suggestion): Partition column explicitly aliased as `run_date`
--   (not `report_date`) to stay consistent with config + notebook.
-- QA Fix (suggestion): top_n driven by widget parameter, not a hardcoded 10.
-- =============================================================================

-- ── Step 1: Resolve target date (injected by notebook widget) ─────────────────
-- report_date widget is set to yesterday by the notebook; this CTE makes the
-- value referenceable across the query without repeating the expression.
WITH target_date AS (
    SELECT
        CAST('${run_date}' AS DATE) AS report_date
),

-- ── Step 2: Filter viewing sessions to the target date ───────────────────────
daily_sessions AS (
    SELECT
        vs.subscriber_id,
        vs.session_start,
        vs.session_end,
        vs.duration_minutes
    FROM
        viewing_sessions  vs
    CROSS JOIN
        target_date       td
    WHERE
        CAST(vs.session_start AS DATE) = td.report_date
        AND vs.duration_minutes IS NOT NULL
        AND vs.duration_minutes >= 0        -- guard against corrupt rows
),

-- ── Step 3: Aggregate total watch time per subscriber ────────────────────────
subscriber_watch_time AS (
    SELECT
        subscriber_id,
        SUM(duration_minutes)               AS total_watch_time_minutes,
        COUNT(*)                            AS session_count
    FROM
        daily_sessions
    GROUP BY
        subscriber_id
),

-- ── Step 4: Enrich with subscriber metadata ──────────────────────────────────
enriched AS (
    SELECT
        sp.subscriber_id,
        sp.name                             AS subscriber_name,
        sp.plan                             AS subscription_plan,
        swt.total_watch_time_minutes,
        swt.session_count
    FROM
        subscriber_watch_time               swt
    INNER JOIN
        subscriber_profiles                 sp
          ON swt.subscriber_id = sp.subscriber_id
),

-- ── Step 5: Rank subscribers by total watch time (desc) ──────────────────────
-- DENSE_RANK ensures no gaps in rank; ROW_NUMBER used for deterministic tie-
-- breaking (by subscriber_id ascending) so exactly top_n rows are selected.
ranked AS (
    SELECT
        subscriber_id,
        subscriber_name,
        subscription_plan,
        total_watch_time_minutes,
        session_count,
        DENSE_RANK() OVER (
            ORDER BY total_watch_time_minutes DESC
        )                                   AS watch_time_rank,
        ROW_NUMBER() OVER (
            ORDER BY total_watch_time_minutes DESC,
                     subscriber_id           ASC   -- deterministic tie-break
        )                                   AS row_num
    FROM
        enriched
)

-- ── Step 6: Select top-N and attach partition column ─────────────────────────
-- QA Fix (critical): run_date is injected via ${run_date} widget (== Airflow
--   {{ ds }}) so backfill runs always target the correct date.
-- QA Fix (suggestion): aliased as `run_date` (not report_date) to match the
--   partition_column declared in top_10_subscribers_by_watch_time_config.yml.
SELECT
    td.report_date                          AS run_date,   -- partition column
    r.watch_time_rank,
    r.row_num,
    r.subscriber_id,
    r.subscriber_name,
    r.subscription_plan,
    r.total_watch_time_minutes,
    r.session_count,
    CURRENT_TIMESTAMP()                     AS pipeline_run_ts
FROM
    ranked          r
CROSS JOIN
    target_date     td
WHERE
    r.row_num <= CAST('${top_n}' AS INT)    -- driven by config widget, not hardcoded
ORDER BY
    r.watch_time_rank ASC,
    r.subscriber_id   ASC
