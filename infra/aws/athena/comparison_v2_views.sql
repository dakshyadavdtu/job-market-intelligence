-- comparison_v2_views.sql — Gold legacy derived_* cleanup only (no jmi_analytics_v2 wrappers).
-- Comparison views live in docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql.

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
