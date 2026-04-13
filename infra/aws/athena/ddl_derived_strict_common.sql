-- Physical materialized strict common month layer (Parquet under derived/comparison/strict_common_month/).
-- Replace BUCKET placeholder or set JMI_BUCKET when deploying.

CREATE DATABASE IF NOT EXISTS jmi_analytics_v2;

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_analytics_v2.derived_strict_common_manifest (
  layer_scope string,
  run_id_arbeitnow string,
  run_id_adzuna_in string,
  strict_months_csv string,
  strict_intersection_month_count bigint,
  strict_intersection_latest_month string,
  march_in_strict_intersection boolean,
  materialized_at_utc string
)
STORED AS PARQUET
LOCATION 's3://BUCKET/derived/comparison/strict_common_month/manifest/';

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_analytics_v2.derived_strict_common_month_totals (
  source string,
  posted_month string,
  run_id string,
  total_postings bigint,
  layer_scope string
)
STORED AS PARQUET
LOCATION 's3://BUCKET/derived/comparison/strict_common_month/month_totals/';

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_analytics_v2.derived_strict_common_benchmark_summary (
  layer_scope string,
  strict_intersection_latest_month string,
  strict_intersection_month_count bigint,
  march_strict_comparable_both_sources boolean,
  run_id_arbeitnow string,
  run_id_adzuna_in string,
  materialized_at_utc string
)
STORED AS PARQUET
LOCATION 's3://BUCKET/derived/comparison/strict_common_month/benchmark_summary/';

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_analytics_v2.derived_strict_common_skill_mix (
  skill string,
  job_count bigint,
  bronze_ingest_date string,
  bronze_run_id string,
  time_axis string,
  source string,
  posted_month string,
  run_id string,
  layer_scope string
)
STORED AS PARQUET
LOCATION 's3://BUCKET/derived/comparison/strict_common_month/skill_mix/';

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_analytics_v2.derived_strict_common_role_mix (
  role string,
  job_count bigint,
  bronze_ingest_date string,
  bronze_run_id string,
  time_axis string,
  source string,
  posted_month string,
  run_id string,
  layer_scope string
)
STORED AS PARQUET
LOCATION 's3://BUCKET/derived/comparison/strict_common_month/role_mix/';
