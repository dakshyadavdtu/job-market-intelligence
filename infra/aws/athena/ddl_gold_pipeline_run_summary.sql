-- Partition projection matches other Gold monthly tables so new S3 prefixes resolve
-- without MSCK. Latest run_id for dashboards comes from jmi_gold.latest_run_metadata
-- (written each run); views filter by that run_id and constrain ingest_month within
-- projection.ingest_month.range (see docs/dashboard_implementation/ATHENA_VIEWS.sql).
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
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/pipeline_run_summary/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.ingest_month.type' = 'date',
  'projection.ingest_month.format' = 'yyyy-MM',
  'projection.ingest_month.interval' = '1',
  'projection.ingest_month.interval.unit' = 'MONTHS',
  'projection.ingest_month.range' = '2018-01,2035-12',
  'projection.run_id.type' = 'injected',
  'storage.location.template' = 's3://jmi-dakshyadav-job-market-intelligence/gold/pipeline_run_summary/ingest_month=${ingest_month}/run_id=${run_id}/'
);
