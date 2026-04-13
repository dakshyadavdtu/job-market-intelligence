-- jmi_silver_v2: Adzuna India merged Silver snapshot (one Parquet file per source).
-- Path: silver/jobs/source=adzuna_in/merged/latest.parquet
-- skills is stored as a JSON array string (matches pipeline project_silver_to_contract).
CREATE DATABASE IF NOT EXISTS jmi_silver_v2;

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_silver_v2.adzuna_jobs_merged (
  job_id string,
  source string,
  source_job_id string,
  title_norm string,
  company_norm string,
  location_raw string,
  remote_type string,
  skills string,
  posted_at string,
  ingested_at string,
  job_id_strategy string,
  bronze_run_id string,
  bronze_ingest_date string,
  bronze_data_file string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/silver/jobs/source=adzuna_in/merged/';
