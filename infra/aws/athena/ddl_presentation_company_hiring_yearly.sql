CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.presentation_company_hiring_yearly (
  company_name string,
  job_count bigint,
  bronze_ingest_date string,
  bronze_run_id string,
  time_axis string,
  presentation_build_id string,
  source_gold_run_id string,
  calendar_year string
)
PARTITIONED BY (
  source string,
  year string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold_v2/presentation/v2_company_hiring/yearly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'arbeitnow,adzuna_in',
  'projection.year.type' = 'integer',
  'projection.year.range' = '2015,2035',
  'projection.year.interval' = '1'
);
