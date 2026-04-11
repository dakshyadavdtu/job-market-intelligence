-- Partition projection is NOT enabled here on purpose: analytics views resolve the latest
-- pipeline run via SELECT MAX(run_id) FROM this table. That pattern requires Glue-registered
-- partitions (not injected projection on run_id). After each Gold run that writes new
-- ingest_month/run_id prefixes, register partitions (MSCK REPAIR or Glue Crawler) for
-- this table only — see docs/dashboard_implementation/QUICKSIGHT_BUILD_CHECKLIST.md.
CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.pipeline_run_summary (
  source string,
  bronze_ingest_date string,
  bronze_run_id string,
  skill_row_count bigint,
  role_row_count bigint,
  location_row_count bigint,
  company_row_count bigint,
  status string
)
PARTITIONED BY (
  ingest_month string,
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/pipeline_run_summary/';
