CREATE DATABASE IF NOT EXISTS jmi_silver;
CREATE DATABASE IF NOT EXISTS jmi_gold;

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_silver.jobs (
  job_id string,
  source string,
  source_job_id string,
  title_raw string,
  title_norm string,
  company_raw string,
  company_norm string,
  location_raw string,
  location_city string,
  location_country string,
  remote_type string,
  employment_type string,
  category string,
  description_text string,
  skills array<string>,
  salary_min double,
  salary_max double,
  salary_currency string,
  posted_at string,
  ingested_at string,
  record_status string,
  raw_url string,
  job_id_strategy string,
  schema_version string,
  source_record_key string,
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
