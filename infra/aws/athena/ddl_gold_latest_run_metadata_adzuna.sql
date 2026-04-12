-- Single-row pointer for latest Adzuna India Gold run (optional; mirrors latest_run_metadata for EU).
-- Path: gold/source=adzuna_in/latest_run_metadata/part-00001.parquet
CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.latest_run_metadata_adzuna (
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/source=adzuna_in/latest_run_metadata/';
