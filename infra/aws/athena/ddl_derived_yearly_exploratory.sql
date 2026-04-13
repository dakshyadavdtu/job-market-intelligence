-- Materialized yearly exploratory rollup: EXTERNAL TABLEs in jmi_gold_v2 (Gold-level).
-- VIEWs in jmi_analytics_v2 for dashboard access (analytics stays view-only).

CREATE DATABASE IF NOT EXISTS jmi_gold_v2;

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_v2.derived_yearly_exploratory_manifest (
  layer_scope string,
  exploratory_only boolean,
  distinct_calendar_years_union_csv string,
  distinct_year_count_union bigint,
  multi_calendar_year_data_present boolean,
  headline_multi_year_narrative_worthy boolean,
  note string,
  materialized_at_utc string
)
STORED AS PARQUET
LOCATION 's3://BUCKET/derived/comparison/yearly/manifest/';

CREATE EXTERNAL TABLE IF NOT EXISTS jmi_gold_v2.derived_yearly_exploratory_source_year_totals (
  source string,
  calendar_year bigint,
  total_postings bigint,
  months_present_in_year bigint,
  run_id string,
  layer_scope string,
  data_alignment string,
  exploratory_only boolean,
  materialized_at_utc string
)
STORED AS PARQUET
LOCATION 's3://BUCKET/derived/comparison/yearly/exploratory_source_year_totals/';

-- Analytics views (jmi_analytics_v2 = views only)

CREATE DATABASE IF NOT EXISTS jmi_analytics_v2;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_yearly_exploratory_manifest AS
SELECT * FROM jmi_gold_v2.derived_yearly_exploratory_manifest;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_yearly_exploratory_source_year_totals AS
SELECT * FROM jmi_gold_v2.derived_yearly_exploratory_source_year_totals;
