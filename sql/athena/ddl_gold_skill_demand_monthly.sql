CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold.skill_demand_monthly (
  skill string,
  job_count bigint
)
PARTITIONED BY (ingest_month string)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/gold/skill_demand_monthly/';

-- Add partitions manually in MVP to control costs:
-- ALTER TABLE jmi_gold.skill_demand_monthly
-- ADD PARTITION (ingest_month='2026-03')
-- LOCATION 's3://<your-bucket>/gold/skill_demand_monthly/ingest_month=2026-03/';
