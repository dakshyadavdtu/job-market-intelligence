CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.presentation_pipeline_run_summary_monthly (
  bronze_ingest_date string,
  bronze_run_id string,
  skill_row_count bigint,
  role_row_count bigint,
  location_row_count bigint,
  company_row_count bigint,
  status string,
  time_axis string,
  presentation_build_id string,
  source_gold_run_id string
)
PARTITIONED BY (
  source string,
  posted_month string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold_v2/presentation/v2_pipeline_run_summary/monthly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'arbeitnow,adzuna_in',
  'projection.posted_month.type' = 'date',
  'projection.posted_month.format' = 'yyyy-MM',
  'projection.posted_month.interval' = '1',
  'projection.posted_month.interval.unit' = 'MONTHS',
  'projection.posted_month.range' = '2018-01,2035-12'
);
