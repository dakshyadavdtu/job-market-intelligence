-- Single-row pointer for latest Arbeitnow Gold run (EU).
-- Path: gold/source=arbeitnow/latest_run_metadata/part-00001.parquet
CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.latest_run_metadata_arbeitnow (
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/source=arbeitnow/latest_run_metadata/';
