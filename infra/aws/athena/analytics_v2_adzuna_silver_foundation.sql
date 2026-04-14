-- jmi_analytics_v2: reusable Adzuna Silver-backed foundations (v2 only).
-- Rows are restricted to posted_month values present in the latest Adzuna Gold run so BI (QuickSight)
-- does not scan the full merged Silver history on every query.
-- Depends on: jmi_silver_v2.adzuna_jobs_merged, jmi_gold_v2.latest_run_metadata_adzuna

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_silver_jobs_base AS
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
  FROM jmi_silver_v2.adzuna_jobs_merged
  WHERE source = 'adzuna_in'
) b
WHERE b.posted_month IN (
  SELECT DISTINCT r.posted_month
  FROM jmi_gold_v2.role_demand_monthly r
  INNER JOIN (SELECT run_id FROM jmi_gold_v2.latest_run_metadata_adzuna LIMIT 1) lr ON r.run_id = lr.run_id
  WHERE r.source = 'adzuna_in'
);

-- UNNEST expands one row per skill tag; heavy at scale — prefer v2_in_gold_skill_rows_monthly for Direct Query.
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_silver_jobs_skills_long AS
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
FROM jmi_analytics_v2.v2_in_silver_jobs_base b
CROSS JOIN UNNEST(
  COALESCE(
    TRY_CAST(json_parse(b.skills_json) AS array(varchar)),
    CAST(ARRAY[] AS array(varchar))
  )
) AS t (skill_token)
WHERE trim(COALESCE(t.skill_token, '')) <> '';
