-- =============================================================================
-- Pipeline  : weekly_most_watched_content_by_view_count
-- Layer     : Gold
-- Schedule  : Every Monday at 06:00 UTC  (cron: 0 6 * * 1)
-- Output    : gold_most_watched_content_by_view_count_weekly
--             partitioned by week_start_date
--
-- PURPOSE
-- -------
-- Ranks every piece of content by distinct-session view count for the
-- previous completed ISO week (Monday–Sunday).  The result is enriched
-- with title, genre, content_type, and release_year from content_catalog.
--
-- REFERENCE / DOCUMENTATION ARTIFACT
-- ------------------------------------
-- This SQL file is the canonical, human-readable specification of the
-- transformation logic.  The Databricks notebook
-- (weekly_most_watched_content_by_view_count_notebook.py) executes the
-- equivalent logic via PySpark / spark.sql().  The two implementations
-- must be kept in sync; this file is NOT directly executed by the pipeline.
-- If you need to run it ad-hoc, substitute the bind parameters below with
-- literal date strings (e.g. replace :week_start_date with '2025-01-06').
--
-- BIND PARAMETERS (ad-hoc execution only)
--   :week_start_date  DATE  — Monday of the reporting week  e.g. 2025-01-06
--   :week_end_date    DATE  — Sunday of the reporting week  e.g. 2025-01-12
--   :top_n            INT   — Optional row cap (NULL = no cap)
-- =============================================================================

WITH

-- ---------------------------------------------------------------------------
-- Step 1 — Filter viewing_sessions to the previous completed ISO week
-- ---------------------------------------------------------------------------
weekly_sessions AS (
    SELECT
        session_id,
        content_id,
        session_date                        -- DATE column; adjust if TIMESTAMP
    FROM viewing_sessions
    WHERE
        session_date >= :week_start_date    -- Monday 00:00 (inclusive)
        AND session_date <  :week_end_date  -- following Monday 00:00 (exclusive)
        -- Exclude nulls that would inflate COUNT DISTINCT
        AND session_id  IS NOT NULL
        AND content_id  IS NOT NULL
),

-- ---------------------------------------------------------------------------
-- Step 2 — Aggregate: one view = one distinct session_id per content_id
-- ---------------------------------------------------------------------------
content_view_counts AS (
    SELECT
        content_id,
        COUNT(DISTINCT session_id)  AS view_count
    FROM weekly_sessions
    GROUP BY content_id
),

-- ---------------------------------------------------------------------------
-- Step 3 — Enrich with content metadata from content_catalog
-- ---------------------------------------------------------------------------
enriched AS (
    SELECT
        vc.content_id,
        cc.title,
        cc.genre,
        cc.content_type,
        cc.release_year,
        vc.view_count
    FROM content_view_counts  vc
    -- LEFT JOIN retains content that exists in sessions but may be missing
    -- from the catalog (orphan IDs).  Downstream DQ flags these via NULL title.
    LEFT JOIN content_catalog cc
        ON vc.content_id = cc.content_id
),

-- ---------------------------------------------------------------------------
-- Step 4 — Rank by view_count DESC
--          DENSE_RANK  → ties share the same rank position
--          ROW_NUMBER  → deterministic tie-break (arbitrary but stable)
-- ---------------------------------------------------------------------------
ranked AS (
    SELECT
        content_id,
        title,
        genre,
        content_type,
        release_year,
        view_count,
        DENSE_RANK()  OVER (ORDER BY view_count DESC)  AS view_count_rank,
        ROW_NUMBER()  OVER (ORDER BY view_count DESC,
                                     content_id  ASC)  AS row_num
    FROM enriched
),

-- ---------------------------------------------------------------------------
-- Step 5 — Attach partition column and apply optional top-N cap
-- ---------------------------------------------------------------------------
final AS (
    SELECT
        content_id,
        title,
        genre,
        content_type,
        release_year,
        view_count,
        view_count_rank,
        row_num,
        CAST(:week_start_date AS DATE)     AS week_start_date   -- partition key
    FROM ranked
    WHERE
        -- top_n = NULL means no cap (all ranked content written)
        (:top_n IS NULL OR view_count_rank <= :top_n)
)

-- ---------------------------------------------------------------------------
-- Final SELECT — consumed by the notebook's spark.sql() or ad-hoc tooling
-- ---------------------------------------------------------------------------
SELECT
    content_id,
    title,
    genre,
    content_type,
    release_year,
    view_count,
    view_count_rank,
    row_num,
    week_start_date
FROM final
-- NOTE: ORDER BY is intentionally omitted here.
-- Delta Lake does not preserve DataFrame/result-set order on disk.
-- Consumers must apply ORDER BY at query time.
-- Physical layout is optimised by ZORDER BY (content_id) in the notebook.
ORDER BY
    view_count_rank ASC,
    row_num         ASC
;
