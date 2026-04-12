-- Partition projection: queries must filter ingest_month within projection.ingest_month.range below
-- and run_id (see docs/dashboard_implementation/ATHENA_VIEWS.sql). No MSCK REPAIR for new run_id paths.
-- Do not set storage.location.template: Athena defaults to Hive-style paths under LOCATION
-- (ingest_month=<val>/run_id=<val>/), matching transform_gold.py S3 layout.
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
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/skill_demand_monthly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.ingest_month.type' = 'date',
  'projection.ingest_month.format' = 'yyyy-MM',
  'projection.ingest_month.interval' = '1',
  'projection.ingest_month.interval.unit' = 'MONTHS',
  'projection.ingest_month.range' = '2018-01,2035-12',
  'projection.run_id.type' = 'injected'
);

