-- comparison_v2_views.sql — Demo-pruned: strict-common month totals + benchmark summary only.
-- Prerequisites: deploy_athena_comparison_views_v2.py (comparison_* views) and jmi_gold_v2 facts.
-- v2_strict_common_* run_id_* columns use the Gold run_id for strict_intersection_latest_month (per-month MAX(run_id)), not the global latest_run_metadata_arbeitnow pointer.
-- Run order: CREATE OR REPLACE VIEW statements first, then DROP TABLE for obsolete external tables.

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_strict_common_month_totals AS
SELECT
  source,
  posted_month,
  run_id,
  total_postings,
  CAST('strict_common_month' AS VARCHAR) AS layer_scope
FROM jmi_analytics_v2.comparison_strict_intersection_month_totals;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_strict_common_benchmark_summary AS
WITH pol AS (SELECT * FROM jmi_analytics_v2.comparison_time_window_policy LIMIT 1)
SELECT
  CAST('strict_common_month' AS VARCHAR) AS layer_scope,
  p.strict_intersection_latest_month,
  p.strict_intersection_month_count,
  p.march_strict_comparable_both_sources,
  (SELECT run_id FROM jmi_analytics_v2.comparison_strict_intersection_month_totals t
   WHERE t.source = 'arbeitnow'
     AND t.posted_month = (SELECT MAX(m.posted_month) FROM jmi_analytics_v2.comparison_strict_intersection_months m)
   LIMIT 1) AS run_id_arbeitnow,
  (SELECT run_id FROM jmi_analytics_v2.comparison_strict_intersection_month_totals t
   WHERE t.source = 'adzuna_in'
     AND t.posted_month = (SELECT MAX(m.posted_month) FROM jmi_analytics_v2.comparison_strict_intersection_months m)
   LIMIT 1) AS run_id_adzuna_in,
  date_format(at_timezone(current_timestamp, 'UTC'), '%Y-%m-%dT%H:%i:%SZ') AS materialized_at_utc
FROM pol p;

-- -----------------------------------------------------------------------------
-- Drop obsolete physical comparison tables in jmi_gold_v2 (S3 blobs may remain orphaned).
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS jmi_gold_v2.derived_strict_common_manifest;
DROP TABLE IF EXISTS jmi_gold_v2.derived_strict_common_month_totals;
DROP TABLE IF EXISTS jmi_gold_v2.derived_strict_common_benchmark_summary;
DROP TABLE IF EXISTS jmi_gold_v2.derived_strict_common_skill_mix;
DROP TABLE IF EXISTS jmi_gold_v2.derived_strict_common_role_mix;
DROP TABLE IF EXISTS jmi_gold_v2.derived_march_strict_manifest;
DROP TABLE IF EXISTS jmi_gold_v2.derived_march_strict_month_totals;
DROP TABLE IF EXISTS jmi_gold_v2.derived_march_strict_benchmark_summary;
DROP TABLE IF EXISTS jmi_gold_v2.derived_march_strict_skill_mix;
DROP TABLE IF EXISTS jmi_gold_v2.derived_march_strict_role_mix;
DROP TABLE IF EXISTS jmi_gold_v2.derived_yearly_exploratory_manifest;
DROP TABLE IF EXISTS jmi_gold_v2.derived_yearly_exploratory_source_year_totals;
