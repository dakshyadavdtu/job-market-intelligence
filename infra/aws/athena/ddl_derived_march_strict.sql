-- Materialized March strict layer: EXTERNAL TABLEs in jmi_gold_v2 (Gold-level).
-- VIEWs in jmi_analytics_v2 (analytics stays view-only).

CREATE DATABASE IF NOT EXISTS jmi_gold_v2;

DROP TABLE IF EXISTS jmi_gold_v2.derived_march_strict_manifest;
DROP TABLE IF EXISTS jmi_gold_v2.derived_march_strict_month_totals;
DROP TABLE IF EXISTS jmi_gold_v2.derived_march_strict_benchmark_summary;
DROP TABLE IF EXISTS jmi_gold_v2.derived_march_strict_skill_mix;
DROP TABLE IF EXISTS jmi_gold_v2.derived_march_strict_role_mix;

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_v2.derived_march_strict_manifest (
  layer_scope string,
  run_id_arbeitnow string,
  run_id_adzuna_in string,
  march_posted_months_csv string,
  march_month_count bigint,
  strict_intersection_superset_csv string,
  materialized_at_utc string
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/comparison_march_only/manifest/';

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_v2.derived_march_strict_month_totals (
  source string,
  posted_month string,
  run_id string,
  total_postings bigint,
  layer_scope string
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/comparison_march_only/month_totals/';

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_v2.derived_march_strict_benchmark_summary (
  layer_scope string,
  march_strict_latest_month string,
  march_month_count bigint,
  run_id_arbeitnow string,
  run_id_adzuna_in string,
  materialized_at_utc string
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/comparison_march_only/benchmark_summary/';

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_v2.derived_march_strict_skill_mix (
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
LOCATION 's3://BUCKET/gold/comparison_march_only/skill_mix/';

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_v2.derived_march_strict_role_mix (
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
LOCATION 's3://BUCKET/gold/comparison_march_only/role_mix/';

-- Analytics views (jmi_analytics_v2 = views only)

CREATE DATABASE IF NOT EXISTS jmi_analytics_v2;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_march_strict_manifest AS
SELECT * FROM jmi_gold_v2.derived_march_strict_manifest;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_march_strict_month_totals AS
SELECT * FROM jmi_gold_v2.derived_march_strict_month_totals;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_march_strict_benchmark_summary AS
SELECT * FROM jmi_gold_v2.derived_march_strict_benchmark_summary;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_march_strict_skill_mix AS
SELECT * FROM jmi_gold_v2.derived_march_strict_skill_mix;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_march_strict_role_mix AS
SELECT * FROM jmi_gold_v2.derived_march_strict_role_mix;
