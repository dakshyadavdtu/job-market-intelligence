-- jmi_analytics_v2: Europe (Arbeitnow) Silver-backed row foundation (v2 only).
-- Depends on: jmi_silver_v2.arbeitnow_jobs_merged, jmi_gold_v2.role_demand_monthly
-- Rows are restricted to:
--   (1) rolling previous + current UTC calendar month (live dashboard window), and
--   (2) posted_month values that exist in EU Gold (any run_id) in that window — so March is included
--   when historically materialized, not only when the single latest_run_metadata run has that month.
-- Does not replace v2_eu_role_titles_classified / v2_eu_employers_top_clean (Gold-derived analytics).
-- v2_eu_silver_jobs_skills_long: UNNEST expands one row per skill tag; prefer v2_eu_gold_skill_rows_monthly if too heavy.

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_silver_jobs_base AS
WITH month_bounds AS (
  SELECT
    date_format(date_add('month', -1, date_trunc('month', current_timestamp)), '%Y-%m') AS pm_min,
    date_format(date_trunc('month', current_timestamp), '%Y-%m') AS pm_max
),
gold_months AS (
  SELECT DISTINCT r.posted_month
  FROM jmi_gold_v2.role_demand_monthly r
  CROSS JOIN month_bounds b
  WHERE r.source = 'arbeitnow'
    AND r.posted_month BETWEEN b.pm_min AND b.pm_max
)
SELECT
  job_id,
  source,
  source_job_id,
  title_norm,
  company_norm,
  location_raw,
  remote_type,
  posted_at,
  posted_month,
  ingested_at,
  bronze_ingest_date,
  bronze_run_id,
  job_id_strategy,
  bronze_data_file,
  skills_json
FROM (
  SELECT
    job_id,
    source,
    source_job_id,
    title_norm,
    company_norm,
    location_raw,
    remote_type,
    posted_at,
    date_format(
      date_trunc(
        'month',
        COALESCE(
          TRY(date_parse(nullif(trim(substr(posted_at, 1, 10)), ''), '%Y-%m-%d')),
          TRY(cast(from_iso8601_timestamp(posted_at) AS date))
        )
      ),
      '%Y-%m'
    ) AS posted_month,
    ingested_at,
    bronze_ingest_date,
    bronze_run_id,
    job_id_strategy,
    bronze_data_file,
    skills AS skills_json
  FROM jmi_silver_v2.arbeitnow_jobs_merged
  WHERE source = 'arbeitnow'
) b
WHERE b.posted_month IN (SELECT g.posted_month FROM gold_months g);

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_silver_jobs_skills_long AS
SELECT
  b.job_id,
  b.source,
  b.source_job_id,
  b.title_norm,
  b.company_norm,
  b.location_raw,
  b.remote_type,
  b.posted_at,
  b.posted_month,
  b.ingested_at,
  b.bronze_ingest_date,
  b.bronze_run_id,
  trim(t.skill_token) AS skill_token
FROM jmi_analytics_v2.v2_eu_silver_jobs_base b
CROSS JOIN UNNEST(
  COALESCE(
    TRY_CAST(json_parse(b.skills_json) AS array(varchar)),
    CAST(ARRAY[] AS array(varchar))
  )
) AS t (skill_token)
WHERE trim(COALESCE(t.skill_token, '')) <> '';
