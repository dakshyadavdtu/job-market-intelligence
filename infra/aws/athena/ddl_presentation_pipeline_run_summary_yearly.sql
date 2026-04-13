CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.presentation_pipeline_run_summary_yearly (
  calendar_year string,
  skill_row_count bigint,
  role_row_count bigint,
  location_row_count bigint,
  company_row_count bigint,
  bronze_ingest_date string,
  time_axis string,
  status string,
  presentation_build_id string,
  source_gold_run_id string
)
PARTITIONED BY (
  source string,
  year string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold_v2/presentation/v2_pipeline_run_summary/yearly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'arbeitnow,adzuna_in',
  'projection.year.type' = 'integer',
  'projection.year.range' = '2015,2035',
  'projection.year.interval' = '1'
);
