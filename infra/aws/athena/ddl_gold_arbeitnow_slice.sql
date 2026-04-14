-- Glue registration for Arbeitnow slice Gold (JMI_ARBEITNOW_SLICE=arbeitnow_2026_q1_focus).
-- S3 layout: gold/slice=arbeitnow_2026_q1_focus/<table>/source=arbeitnow/posted_month=YYYY-MM/run_id=<id>/part-00001.parquet
-- Same partition semantics as jmi_gold_v2 facts, different LOCATION root (slice isolation).

CREATE DATABASE IF NOT EXISTS jmi_gold_arbeitnow_slice;

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_arbeitnow_slice.skill_demand_monthly (
  skill string,
  job_count bigint,
  bronze_ingest_date string,
  bronze_run_id string,
  time_axis string
)
PARTITIONED BY (
  source string,
  posted_month string,
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/slice=arbeitnow_2026_q1_focus/skill_demand_monthly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'arbeitnow',
  'projection.posted_month.type' = 'date',
  'projection.posted_month.format' = 'yyyy-MM',
  'projection.posted_month.interval' = '1',
  'projection.posted_month.interval.unit' = 'MONTHS',
  'projection.posted_month.range' = '2018-01,2035-12',
  'projection.run_id.type' = 'enum',
  'projection.run_id.values' = '20260414T025526Z-64a0d388'
);

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_arbeitnow_slice.role_demand_monthly (
  role string,
  job_count bigint,
  bronze_ingest_date string,
  bronze_run_id string,
  time_axis string
)
PARTITIONED BY (
  source string,
  posted_month string,
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/slice=arbeitnow_2026_q1_focus/role_demand_monthly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'arbeitnow',
  'projection.posted_month.type' = 'date',
  'projection.posted_month.format' = 'yyyy-MM',
  'projection.posted_month.interval' = '1',
  'projection.posted_month.interval.unit' = 'MONTHS',
  'projection.posted_month.range' = '2018-01,2035-12',
  'projection.run_id.type' = 'enum',
  'projection.run_id.values' = '20260414T025526Z-64a0d388'
);

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_arbeitnow_slice.location_demand_monthly (
  location string,
  job_count bigint,
  bronze_ingest_date string,
  bronze_run_id string,
  time_axis string
)
PARTITIONED BY (
  source string,
  posted_month string,
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/slice=arbeitnow_2026_q1_focus/location_demand_monthly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'arbeitnow',
  'projection.posted_month.type' = 'date',
  'projection.posted_month.format' = 'yyyy-MM',
  'projection.posted_month.interval' = '1',
  'projection.posted_month.interval.unit' = 'MONTHS',
  'projection.posted_month.range' = '2018-01,2035-12',
  'projection.run_id.type' = 'enum',
  'projection.run_id.values' = '20260414T025526Z-64a0d388'
);

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_arbeitnow_slice.company_hiring_monthly (
  company_name string,
  job_count bigint,
  bronze_ingest_date string,
  bronze_run_id string,
  time_axis string
)
PARTITIONED BY (
  source string,
  posted_month string,
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/slice=arbeitnow_2026_q1_focus/company_hiring_monthly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'arbeitnow',
  'projection.posted_month.type' = 'date',
  'projection.posted_month.format' = 'yyyy-MM',
  'projection.posted_month.interval' = '1',
  'projection.posted_month.interval.unit' = 'MONTHS',
  'projection.posted_month.range' = '2018-01,2035-12',
  'projection.run_id.type' = 'enum',
  'projection.run_id.values' = '20260414T025526Z-64a0d388'
);

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_arbeitnow_slice.pipeline_run_summary (
  bronze_ingest_date string,
  bronze_run_id string,
  skill_row_count bigint,
  role_row_count bigint,
  location_row_count bigint,
  company_row_count bigint,
  status string,
  time_axis string
)
PARTITIONED BY (
  source string,
  posted_month string,
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/slice=arbeitnow_2026_q1_focus/pipeline_run_summary/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'arbeitnow',
  'projection.posted_month.type' = 'date',
  'projection.posted_month.format' = 'yyyy-MM',
  'projection.posted_month.interval' = '1',
  'projection.posted_month.interval.unit' = 'MONTHS',
  'projection.posted_month.range' = '2018-01,2035-12',
  'projection.run_id.type' = 'enum',
  'projection.run_id.values' = '20260414T025526Z-64a0d388'
);

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_arbeitnow_slice.latest_run_metadata (
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/slice=arbeitnow_2026_q1_focus/source=arbeitnow/latest_run_metadata/';
