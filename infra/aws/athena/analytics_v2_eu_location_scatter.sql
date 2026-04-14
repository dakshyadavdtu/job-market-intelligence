-- Europe (Arbeitnow): location × month with volume + share for scatter / bubble charts.
-- x = location_job_count, y = location_share_of_monthly_total (bubble size optional = same as x or job_count).
-- Uses MAX(run_id) per posted_month in rolling previous+current UTC month (same policy as v2_eu_kpi_slice_monthly).

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_location_scatter_metrics AS
WITH month_bounds AS (
  SELECT
    date_format(date_add('month', -1, date_trunc('month', current_timestamp)), '%Y-%m') AS pm_min,
    date_format(date_trunc('month', current_timestamp), '%Y-%m') AS pm_max
),
month_latest AS (
  SELECT r.posted_month, MAX(r.run_id) AS run_id
  FROM jmi_gold_v2.role_demand_monthly r
  CROSS JOIN month_bounds b
  WHERE r.source = 'arbeitnow'
    AND r.posted_month BETWEEN b.pm_min AND b.pm_max
  GROUP BY r.posted_month
),
base AS (
  SELECT
    l.location AS location_label,
    l.posted_month,
    l.job_count,
    l.run_id
  FROM jmi_gold_v2.location_demand_monthly l
  INNER JOIN month_latest ml ON l.posted_month = ml.posted_month AND l.run_id = ml.run_id
  WHERE l.source = 'arbeitnow'
),
tot AS (
  SELECT run_id, posted_month, SUM(job_count) AS monthly_total
  FROM base
  GROUP BY run_id, posted_month
)
SELECT
  b.location_label,
  b.posted_month,
  b.job_count AS location_job_count,
  b.run_id,
  CAST(b.job_count AS double) / CAST(NULLIF(t.monthly_total, 0) AS double) AS location_share_of_monthly_total
FROM base b
INNER JOIN tot t ON b.run_id = t.run_id AND b.posted_month = t.posted_month;
