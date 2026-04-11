CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.company_hiring_monthly (
  company_name string,
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
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/company_hiring_monthly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.ingest_month.type' = 'date',
  'projection.ingest_month.format' = 'yyyy-MM',
  'projection.ingest_month.interval' = '1',
  'projection.ingest_month.interval.unit' = 'MONTHS',
  'projection.ingest_month.range' = '2018-01,2035-12',
  'projection.run_id.type' = 'injected',
  'storage.location.template' = 's3://jmi-dakshyadav-job-market-intelligence/gold/company_hiring_monthly/ingest_month=${ingest_month}/run_id=${run_id}/'
);
