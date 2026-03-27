CREATE DATABASE IF NOT EXISTS jmi_silver;
CREATE DATABASE IF NOT EXISTS jmi_gold;

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_silver.jobs (
  job_id string,
  job_id_strategy string,
  source_record_key string,
  source string,
  schema_version string,
  title string,
  title_clean string,
  company_name string,
  location string,
  is_remote boolean,
  published_at_raw string,
  skills array<string>,
  posting_url string,
  ingested_at string,
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

