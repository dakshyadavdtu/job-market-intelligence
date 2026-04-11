CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.company_hiring_monthly (
  company_name string,
  job_count bigint,
  source string,
  bronze_ingest_date string,
  bronze_run_id string
)
PARTITIONED BY (
  ingest_month string,
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/company_hiring_monthly/';
