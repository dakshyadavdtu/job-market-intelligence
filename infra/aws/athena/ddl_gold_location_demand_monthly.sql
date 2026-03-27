CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.location_demand_monthly (
  location string,
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
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/location_demand_monthly/';
