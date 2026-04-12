-- =============================================================================
-- validate_gold_projection_fix.sql
-- Run in Athena after fixing Glue metadata on partitioned Gold tables (see
-- docs/aws_live_fix_gold_projection.md). Same workgroup/region as S3/Gold.
-- Projection month bounds MUST match ddl_gold_* TBLPROPERTIES projection.posted_month.range.
-- =============================================================================

-- 1) Latest pipeline run id (from overwritten Parquet metadata)
SELECT 'latest_pipeline_run' AS check_name, run_id AS value
FROM jmi_analytics.latest_pipeline_run;

-- 2) Base Gold: row counts for latest run_id + posted_month within projection range
--    (both predicates required for partition projection to resolve S3 paths.)
SELECT 'gold_skill_demand_monthly_rows' AS check_name, COUNT(*) AS row_count
FROM jmi_gold.skill_demand_monthly
WHERE run_id = (SELECT run_id FROM jmi_analytics.latest_pipeline_run)
  AND posted_month BETWEEN '2018-01' AND '2035-12';

SELECT 'gold_role_demand_monthly_rows' AS check_name, COUNT(*) AS row_count
FROM jmi_gold.role_demand_monthly
WHERE run_id = (SELECT run_id FROM jmi_analytics.latest_pipeline_run)
  AND posted_month BETWEEN '2018-01' AND '2035-12';

SELECT 'gold_location_demand_monthly_rows' AS check_name, COUNT(*) AS row_count
FROM jmi_gold.location_demand_monthly
WHERE run_id = (SELECT run_id FROM jmi_analytics.latest_pipeline_run)
  AND posted_month BETWEEN '2018-01' AND '2035-12';

SELECT 'gold_company_hiring_monthly_rows' AS check_name, COUNT(*) AS row_count
FROM jmi_gold.company_hiring_monthly
WHERE run_id = (SELECT run_id FROM jmi_analytics.latest_pipeline_run)
  AND posted_month BETWEEN '2018-01' AND '2035-12';

SELECT 'gold_pipeline_run_summary_rows' AS check_name, COUNT(*) AS row_count
FROM jmi_gold.pipeline_run_summary
WHERE run_id = (SELECT run_id FROM jmi_analytics.latest_pipeline_run)
  AND posted_month BETWEEN '2018-01' AND '2035-12';

-- 3) Latest-run analytics views (QuickSight-facing)
SELECT 'view_skill_demand_monthly_latest_rows' AS check_name, COUNT(*) AS row_count
FROM jmi_analytics.skill_demand_monthly_latest;

SELECT 'view_pipeline_run_summary_latest_rows' AS check_name, COUNT(*) AS row_count
FROM jmi_analytics.pipeline_run_summary_latest;

SELECT 'view_sheet1_kpis_rows' AS check_name, COUNT(*) AS row_count
FROM jmi_analytics.sheet1_kpis;

-- 4) Spot-check: one row from pipeline summary latest (optional visual sanity)
SELECT 'sample_pipeline_run_summary_latest' AS check_name,
       source, posted_month, run_id, status, skill_row_count, role_row_count
FROM jmi_analytics.pipeline_run_summary_latest
LIMIT 3;
