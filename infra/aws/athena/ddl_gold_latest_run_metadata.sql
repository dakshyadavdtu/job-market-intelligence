-- Single-row Parquet overwritten each Gold run (see src/jmi/pipelines/transform_gold.py).
-- Not partitioned: no MSCK; Athena reads the current object under LOCATION at query time.
-- jmi_analytics.latest_pipeline_run reads run_id from this table.
CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.latest_run_metadata (
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/latest_run_metadata/';
