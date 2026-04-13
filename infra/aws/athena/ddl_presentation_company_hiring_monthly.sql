CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.presentation_company_hiring_monthly (
  company_name string,
  job_count bigint,
  bronze_ingest_date string,
  bronze_run_id string,
  time_axis string,
  presentation_build_id string,
  source_gold_run_id string
)
PARTITIONED BY (
  source string,
  posted_month string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold_v2/presentation/v2_company_hiring/monthly/'
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
