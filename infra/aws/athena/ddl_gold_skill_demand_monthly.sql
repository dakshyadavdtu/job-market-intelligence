-- Modular layout: gold/skill_demand_monthly/source=<source>/posted_month=.../run_id=.../part-00001.parquet
-- posted_month = calendar month of job posting (Silver posted_at), NOT pipeline ingest batch month.
-- Legacy keys may still use ingest_month= — migrate paths and redeploy this DDL with posted_month only.
CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.skill_demand_monthly (
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
LOCATION 's3://jmi-dakshyadav-job-market-intelligence/gold/skill_demand_monthly/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.source.type' = 'enum',
  'projection.source.values' = 'arbeitnow,adzuna_in',
  'projection.posted_month.type' = 'date',
  'projection.posted_month.format' = 'yyyy-MM',
  'projection.posted_month.interval' = '1',
  'projection.posted_month.interval.unit' = 'MONTHS',
  'projection.posted_month.range' = '2018-01,2035-12',
  'projection.run_id.type' = 'enum',
  'projection.run_id.values' = '20260412T024632Z-a951261b,20260412T064632Z-2d7a6775,20260412T102534Z-ca1b73ff,20260412T104501Z-2225d40a'
);
