CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.skill_demand_monthly (
  skill string,
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
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/skill_demand_monthly/';

