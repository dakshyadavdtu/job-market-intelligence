-- Lightweight India (Adzuna) helpers for QuickSight Direct Query.
-- v2_in_silver_remote_classified_monthly: scans narrowed v2_in_silver_jobs_base; prefer SPICE if still slow.
-- v2_in_gold_skill_rows_monthly: Gold skill_demand_monthly only — use instead of v2_in_silver_jobs_skills_long when UNNEST is too heavy.

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_silver_remote_classified_monthly AS
SELECT
  posted_month,
  CAST(
    SUM(CASE WHEN lower(trim(remote_type)) <> 'unknown' THEN 1 ELSE 0 END) AS double
  ) / CAST(NULLIF(COUNT(*), 0) AS double) AS remote_classified_share
FROM jmi_analytics_v2.v2_in_silver_jobs_base
WHERE source = 'adzuna_in'
GROUP BY posted_month;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_gold_skill_rows_monthly AS
SELECT
  s.posted_month,
  s.run_id,
  s.skill,
  s.job_count
FROM jmi_gold_v2.skill_demand_monthly s
INNER JOIN (SELECT run_id FROM jmi_gold_v2.latest_run_metadata_adzuna LIMIT 1) lr ON s.run_id = lr.run_id
WHERE s.source = 'adzuna_in';
