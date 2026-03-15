-- =============================================================================
-- Pipeline  : daily_churned_subscribers_30day_inactivity
-- Layer     : Gold
-- Output    : {catalog}.{schema}.gold_churned_subscribers_30day_inactivity
-- Schedule  : 0 6 * * *  (06:00 UTC daily)
-- Author    : Peacock Data Engineering
-- =============================================================================
-- IMPORTANT: This SQL file is a REFERENCE TEMPLATE only.
--            All table references use {catalog}.{schema} placeholders that MUST
--            be substituted before execution.
--            The Databricks notebook is the AUTHORITATIVE execution path and
--            performs catalog/schema substitution at runtime using validated
--            widget parameters.
--
-- Substitution tokens:
--   {catalog}  → Unity Catalog name  (e.g. dev_catalog, stage_catalog, prod_catalog)
--   {schema}   → Schema / database   (e.g. peacock_core)
--   {run_date} → ISO-8601 date string (e.g. 2024-01-15), validated before use
-- =============================================================================

SELECT
    sp.subscriber_id,
    sp.subscription_plan,
    sp.churn_date,
    MAX(vs.session_start_date)                                      AS last_watch_date,
    DATEDIFF(
        CAST('{run_date}' AS DATE),
        MAX(vs.session_start_date)
    )                                                               AS days_since_last_watch,
    CAST('{run_date}' AS DATE)                                      AS run_date

FROM {catalog}.{schema}.subscriber_profiles  AS sp

LEFT JOIN {catalog}.{schema}.viewing_sessions AS vs
    ON sp.subscriber_id = vs.subscriber_id

WHERE
    sp.is_active = FALSE                          -- churned subscribers only

GROUP BY
    sp.subscriber_id,
    sp.subscription_plan,
    sp.churn_date

HAVING
    -- Include subscribers inactive for more than 30 days
    DATEDIFF(CAST('{run_date}' AS DATE), MAX(vs.session_start_date)) > 30
    -- Also include churned subscribers with NO watch history at all
    OR MAX(vs.session_start_date) IS NULL
;
