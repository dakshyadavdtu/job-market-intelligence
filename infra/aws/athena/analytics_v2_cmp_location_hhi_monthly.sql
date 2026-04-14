-- Month-wise location concentration (HHI) for strict-common comparison months only.
-- Semantics: same rolling window + per-month MAX(run_id) + intersection as
--   jmi_analytics_v2.comparison_strict_intersection_skill_demand / role_demand.
-- HHI = sum_i (share_i^2) where share_i = location job_count / sum(job_count) within (source, posted_month).
-- Sources: arbeitnow | adzuna_in (Gold location_demand_monthly only).

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_cmp_location_hhi_monthly AS
WITH month_bounds AS (
    SELECT
        date_format(date_add('month', -1, date_trunc('month', current_timestamp)), '%Y-%m') AS pm_min,
        date_format(date_trunc('month', current_timestamp), '%Y-%m') AS pm_max
),
month_latest_eu AS (
    SELECT r.posted_month, MAX(r.run_id) AS run_id
    FROM jmi_gold_v2.role_demand_monthly r
    CROSS JOIN month_bounds b
    WHERE r.source = 'arbeitnow'
      AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month
),
month_latest_ad AS (
    SELECT r.posted_month, MAX(r.run_id) AS run_id
    FROM jmi_gold_v2.role_demand_monthly r
    CROSS JOIN month_bounds b
    WHERE r.source = 'adzuna_in'
      AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month
),
intersection AS (
    SELECT e.posted_month
    FROM month_latest_eu e
    INNER JOIN month_latest_ad a ON e.posted_month = a.posted_month
),
loc_strict AS (
    SELECT
        l.source,
        l.location,
        l.job_count,
        l.posted_month,
        l.run_id
    FROM jmi_gold_v2.location_demand_monthly l
    INNER JOIN month_latest_eu m ON l.posted_month = m.posted_month AND l.run_id = m.run_id
    CROSS JOIN month_bounds b
    WHERE l.source = 'arbeitnow'
      AND l.posted_month BETWEEN b.pm_min AND b.pm_max
      AND l.posted_month IN (SELECT i.posted_month FROM intersection i)

    UNION ALL

    SELECT
        l.source,
        l.location,
        l.job_count,
        l.posted_month,
        l.run_id
    FROM jmi_gold_v2.location_demand_monthly l
    INNER JOIN month_latest_ad m ON l.posted_month = m.posted_month AND l.run_id = m.run_id
    CROSS JOIN month_bounds b
    WHERE l.source = 'adzuna_in'
      AND l.posted_month BETWEEN b.pm_min AND b.pm_max
      AND l.posted_month IN (SELECT i.posted_month FROM intersection i)
),
totals AS (
    SELECT
        source,
        posted_month,
        run_id,
        CAST(SUM(job_count) AS double) AS total_jobs
    FROM loc_strict
    GROUP BY source, posted_month, run_id
),
hhi AS (
    SELECT
        l.source,
        l.posted_month,
        l.run_id,
        SUM(
            POWER(
                CAST(l.job_count AS double) / NULLIF(t.total_jobs, 0),
                2
            )
        ) AS location_hhi
    FROM loc_strict l
    INNER JOIN totals t
        ON l.source = t.source
        AND l.posted_month = t.posted_month
        AND l.run_id = t.run_id
    GROUP BY l.source, l.posted_month, l.run_id
)
SELECT
    h.posted_month,
    h.source,
    h.location_hhi,
    h.run_id,
    CAST('strict_common_month' AS VARCHAR) AS layer_scope,
    CAST(TRUE AS BOOLEAN) AS month_in_strict_intersection
FROM hhi h;
