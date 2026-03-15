-- ============================================================
-- Pipeline : top_20_content_by_premium_subscriber_watch_time_monthly
-- Output   : gold_top20_content_by_premium_watch_time_monthly
-- Schedule : 0 8 1 * *  (08:00 UTC, 1st of every month)
-- Scope    : Previous full calendar month, PREMIUM + active
--            subscribers only, top-20 by total watch-time minutes
--            Ties → DENSE_RANK; deterministic cut → ROW_NUMBER
-- Params   : :month_start_date  (YYYY-MM-01)
--            :month_end_date    (last day of previous month)
-- Author   : Peacock Data Engineering
-- ============================================================

WITH

-- ─────────────────────────────────────────────
-- 1. PREMIUM ACTIVE SUBSCRIBERS
--    subscription_plan is case-insensitive 'premium'
--    is_active must be TRUE  (churned / suspended excluded)
-- ─────────────────────────────────────────────
premium_subscribers AS (
    SELECT
        subscriber_id,
        subscription_plan,
        is_active
    FROM subscriber_profiles
    WHERE LOWER(subscription_plan) = 'premium'
      AND is_active = TRUE
),

-- ─────────────────────────────────────────────
-- 2. VALID VIEWING SESSIONS
--    Scoped to the previous full calendar month
--    NULL / zero duration_minutes are excluded
-- ─────────────────────────────────────────────
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
    WHERE vs.session_start_date >= :month_start_date
      AND vs.session_start_date <  :month_end_date        -- exclusive upper bound keeps logic clean
      AND vs.duration_minutes IS NOT NULL
      AND vs.duration_minutes > 0
),

-- ─────────────────────────────────────────────
-- 3. AGGREGATE PER CONTENT
--    total watch-time minutes + unique viewer count
-- ─────────────────────────────────────────────
content_aggregates AS (
    SELECT
        content_id,
        SUM(duration_minutes)           AS total_watch_time_minutes,
        COUNT(DISTINCT subscriber_id)   AS unique_viewers
    FROM filtered_sessions
    GROUP BY content_id
),

-- ─────────────────────────────────────────────
-- 4. ENRICH WITH CONTENT CATALOG METADATA
--    Exclude deprecated content (is_active = FALSE)
-- ─────────────────────────────────────────────
enriched_content AS (
    SELECT
        ca.content_id,
        ca.title,
        ca.genre,
        ca.content_type,
        ca.release_year,
        ca.is_active                        AS content_is_active,
        agg.total_watch_time_minutes,
        agg.unique_viewers
    FROM content_aggregates AS agg
    INNER JOIN content_catalog AS ca
        ON agg.content_id = ca.content_id
    WHERE ca.is_active = TRUE               -- exclude deprecated catalog entries
),

-- ─────────────────────────────────────────────
-- 5. RANKING
--    DENSE_RANK  → tie-safe ranking by watch time DESC
--    ROW_NUMBER  → deterministic tie-break (content_id ASC)
--                  guarantees exactly 20 rows in final output
-- ─────────────────────────────────────────────
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
        )                                   AS watch_time_dense_rank,
        ROW_NUMBER()  OVER (
            ORDER BY total_watch_time_minutes DESC,
                     content_id              ASC   -- deterministic tie-break
        )                                   AS row_num
    FROM enriched_content
)

-- ─────────────────────────────────────────────
-- 6. FINAL OUTPUT — TOP 20
--    Partitioned by month_start_date (injected at runtime)
-- ─────────────────────────────────────────────
SELECT
    CAST(:month_start_date AS DATE)         AS month_start_date,
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
