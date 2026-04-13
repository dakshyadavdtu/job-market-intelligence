-- Adzuna (India) funnel helper — jmi_analytics_v2 only.
-- Depends on: jmi_analytics_v2.v2_in_silver_jobs_base
--
-- This is NOT a hiring / recruitment process funnel. It is a **data coverage** funnel:
-- nested filters on fields that exist in Silver (location, skills JSON, remote_type).
-- Stages are strictly nested (each count <= previous) within (source, posted_month).

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_silver_data_coverage_funnel_monthly AS
WITH base AS (
  SELECT *
  FROM jmi_analytics_v2.v2_in_silver_jobs_base
  WHERE source = 'adzuna_in'
),
agg AS (
  SELECT
    source,
    posted_month,
    max(bronze_run_id) AS latest_bronze_run_id,
    COUNT(*) AS c_all,
    COUNT_IF(trim(location_raw) <> '' AND lower(trim(location_raw)) <> 'india') AS c_subnational,
    COUNT_IF(
      trim(location_raw) <> ''
      AND lower(trim(location_raw)) <> 'india'
      AND COALESCE(cardinality(TRY_CAST(json_parse(skills_json) AS array(varchar))), 0) > 0
    ) AS c_subnational_skills,
    COUNT_IF(
      trim(location_raw) <> ''
      AND lower(trim(location_raw)) <> 'india'
      AND COALESCE(cardinality(TRY_CAST(json_parse(skills_json) AS array(varchar))), 0) > 0
      AND lower(trim(remote_type)) <> 'unknown'
    ) AS c_subnational_skills_remote_known
  FROM base
  GROUP BY source, posted_month
)
SELECT
  CAST('data_coverage' AS varchar) AS funnel_kind,
  CAST(1 AS integer) AS stage_order,
  CAST('1_all_postings' AS varchar) AS stage_name,
  CAST('All ingested postings in the merged Silver snapshot' AS varchar) AS stage_description,
  c_all AS job_count,
  source,
  posted_month,
  latest_bronze_run_id AS bronze_run_id
FROM agg
UNION ALL
SELECT
  CAST('data_coverage' AS varchar),
  CAST(2 AS integer),
  CAST('2_subnational_location' AS varchar),
  CAST('Location is more specific than country-only (excludes empty and india-only)' AS varchar),
  c_subnational,
  source,
  posted_month,
  latest_bronze_run_id
FROM agg
UNION ALL
SELECT
  CAST('data_coverage' AS varchar),
  CAST(3 AS integer),
  CAST('3_subnational_with_skills' AS varchar),
  CAST('Subnational location AND at least one extracted skill token' AS varchar),
  c_subnational_skills,
  source,
  posted_month,
  latest_bronze_run_id
FROM agg
UNION ALL
SELECT
  CAST('data_coverage' AS varchar),
  CAST(4 AS integer),
  CAST('4_remote_mode_classified' AS varchar),
  CAST('Subnational + skills AND remote_type is not unknown (hybrid/onsite/remote)' AS varchar),
  c_subnational_skills_remote_known,
  source,
  posted_month,
  latest_bronze_run_id
FROM agg;
