-- Europe (Arbeitnow): location × month with volume + share for scatter / bubble charts.
-- x = location_job_count, y = location_share_of_monthly_total (bubble size optional = same as x or job_count).
-- Mirrors jmi_analytics.india_city_scatter_metrics; uses jmi_gold_v2 + latest EU run.

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_location_scatter_metrics AS
WITH lr AS (
  SELECT run_id FROM jmi_gold_v2.latest_run_metadata LIMIT 1
),
base AS (
  SELECT
    l.location AS location_label,
    l.posted_month,
    l.job_count,
    l.run_id
  FROM jmi_gold_v2.location_demand_monthly l
  INNER JOIN lr ON l.run_id = lr.run_id
  WHERE l.source = 'arbeitnow'
    AND l.posted_month BETWEEN '2018-01' AND '2035-12'
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
