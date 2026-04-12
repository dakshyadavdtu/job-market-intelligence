-- Modular layout: gold/location_demand_monthly/source=<source>/ingest_month=.../run_id=.../part-00001.parquet
CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.location_demand_monthly (
  location string,
  job_count bigint,
  bronze_ingest_date string,
  bronze_run_id string
)
PARTITIONED BY (
  source string,
  ingest_month string,
  run_id string
)
STORED AS PARQUET
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/location_demand_monthly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'arbeitnow,adzuna_in',
  'projection.ingest_month.type' = 'date',
  'projection.ingest_month.format' = 'yyyy-MM',
  'projection.ingest_month.interval' = '1',
  'projection.ingest_month.interval.unit' = 'MONTHS',
  'projection.ingest_month.range' = '2018-01,2035-12',
  'projection.run_id.type' = 'enum',
  'projection.run_id.values' = '20260412T024632Z-a951261b,20260412T064632Z-2d7a6775,20260412T102534Z-ca1b73ff,20260412T104501Z-2225d40a'
);
