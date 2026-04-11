CREATE DATABASE IF NOT EXISTS jmi_silver;
CREATE DATABASE IF NOT EXISTS jmi_gold;

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_silver.jobs (
  job_id string,
  source string,
  source_job_id string,
  title_norm string,
  company_norm string,
  location_raw string,
  remote_type string,
  employment_type string,
  skills array<string>,
  posted_at string,
  ingested_at string,
  job_id_strategy string,
  bronze_data_file string,
  bronze_run_id string,
  bronze_ingest_date string
)
PARTITIONED BY (
  ingest_date string,
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/silver/jobs/';
