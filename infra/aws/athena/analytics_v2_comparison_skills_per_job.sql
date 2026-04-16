-- jmi_analytics_v2: EU vs India skills-per-job for a fixed calendar month (QuickSight box plot).
-- Depends on: v2_eu_silver_jobs_skills_long, v2_in_silver_jobs_skills_long
-- Semantics: one row per (source, job_id) with >=1 non-blank skill token; skills_per_job matches
-- QuickSight countOver(skill_token, [job_id], PRE_AGG) used in source-specific box plots.
-- Fixed filter: posted_month = '2026-04' only (no March, no yearly rollups).

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_cmp_skills_per_job_april_2026 AS
WITH eu AS (
  SELECT
    job_id,
    source,
    posted_month,
    CAST(COUNT(*) AS BIGINT) AS skills_per_job,
    MAX(bronze_run_id) AS run_id,
    CAST('2026-04' AS VARCHAR) AS time_axis
  FROM jmi_analytics_v2.v2_eu_silver_jobs_skills_long
  WHERE posted_month = '2026-04'
    AND source = 'arbeitnow'
    AND trim(COALESCE(skill_token, '')) <> ''
  GROUP BY job_id, source, posted_month
),
in_rows AS (
  SELECT
    job_id,
    source,
    posted_month,
    CAST(COUNT(*) AS BIGINT) AS skills_per_job,
    MAX(bronze_run_id) AS run_id,
    CAST('2026-04' AS VARCHAR) AS time_axis
  FROM jmi_analytics_v2.v2_in_silver_jobs_skills_long
  WHERE posted_month = '2026-04'
    AND source = 'adzuna_in'
    AND trim(COALESCE(skill_token, '')) <> ''
  GROUP BY job_id, source, posted_month
)
SELECT source, job_id, posted_month, skills_per_job, run_id, time_axis FROM eu
UNION ALL
SELECT source, job_id, posted_month, skills_per_job, run_id, time_axis FROM in_rows;
