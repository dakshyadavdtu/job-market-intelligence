-- Lightweight EU (Arbeitnow) helpers for QuickSight Direct Query.
-- v2_eu_silver_remote_classified_monthly: still scans Silver merged (may be slow); prefer SPICE if needed.
-- v2_eu_gold_skill_rows_monthly: Gold skill_demand_monthly only — use instead of v2_eu_silver_jobs_skills_long when UNNEST is too heavy.

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_silver_remote_classified_monthly AS
SELECT
  posted_month,
  CAST(
    SUM(CASE WHEN lower(trim(remote_type)) <> 'unknown' THEN 1 ELSE 0 END) AS double
  ) / CAST(NULLIF(COUNT(*), 0) AS double) AS remote_classified_share
FROM jmi_analytics_v2.v2_eu_silver_jobs_base
WHERE source = 'arbeitnow'
GROUP BY posted_month;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_gold_skill_rows_monthly AS
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
)
SELECT
  s.posted_month,
  s.run_id,
  s.skill,
  s.job_count
FROM jmi_gold_v2.skill_demand_monthly s
INNER JOIN month_latest ml ON s.posted_month = ml.posted_month AND s.run_id = ml.run_id
WHERE s.source = 'arbeitnow';
