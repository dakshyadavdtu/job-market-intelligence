-- jmi_analytics_v2: EU vs India skills-per-job for a fixed calendar month (QuickSight box plot).
-- Depends on: v2_eu_silver_jobs_base, v2_in_silver_jobs_base, v2_eu_silver_jobs_skills_long, v2_in_silver_jobs_skills_long
-- Semantics: one row per (source, job_id); skills_per_job = COUNT(non-blank skill tokens) from skills_long; 0 if none.
-- Fixed filter: posted_month = '2026-04' only (no March, no yearly rollups).

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_cmp_skills_per_job_april_2026 AS
WITH eu_counts AS (
  SELECT
    job_id,
    source,
    posted_month,
    CAST(COUNT(*) AS BIGINT) AS cnt
  FROM jmi_analytics_v2.v2_eu_silver_jobs_skills_long
  WHERE posted_month = '2026-04'
    AND trim(COALESCE(skill_token, '')) <> ''
  GROUP BY job_id, source, posted_month
),
eu AS (
  SELECT
    b.source,
    b.job_id,
    b.posted_month,
    CAST(COALESCE(c.cnt, 0) AS BIGINT) AS skills_per_job,
    b.bronze_run_id AS run_id,
    CAST('2026-04' AS VARCHAR) AS time_axis
  FROM jmi_analytics_v2.v2_eu_silver_jobs_base b
  LEFT JOIN eu_counts c
    ON b.job_id = c.job_id
    AND b.source = c.source
    AND b.posted_month = c.posted_month
  WHERE b.posted_month = '2026-04'
    AND b.source = 'arbeitnow'
),
in_counts AS (
  SELECT
    job_id,
    source,
    posted_month,
    CAST(COUNT(*) AS BIGINT) AS cnt
  FROM jmi_analytics_v2.v2_in_silver_jobs_skills_long
  WHERE posted_month = '2026-04'
    AND trim(COALESCE(skill_token, '')) <> ''
  GROUP BY job_id, source, posted_month
),
in_rows AS (
  SELECT
    b.source,
    b.job_id,
    b.posted_month,
    CAST(COALESCE(c.cnt, 0) AS BIGINT) AS skills_per_job,
    b.bronze_run_id AS run_id,
    CAST('2026-04' AS VARCHAR) AS time_axis
  FROM jmi_analytics_v2.v2_in_silver_jobs_base b
  LEFT JOIN in_counts c
    ON b.job_id = c.job_id
    AND b.source = c.source
    AND b.posted_month = c.posted_month
  WHERE b.posted_month = '2026-04'
    AND b.source = 'adzuna_in'
)
SELECT source, job_id, posted_month, skills_per_job, run_id, time_axis FROM eu
UNION ALL
SELECT source, job_id, posted_month, skills_per_job, run_id, time_axis FROM in_rows;
