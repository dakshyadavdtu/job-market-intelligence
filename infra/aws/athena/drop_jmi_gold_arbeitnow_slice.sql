-- One-time cleanup if jmi_gold_arbeitnow_slice was created during the slice-only Glue experiment.
-- Run in Athena after main gold_v2 is authoritative. Safe to ignore if the database does not exist.

DROP TABLE IF EXISTS jmi_gold_arbeitnow_slice.skill_demand_monthly;
DROP TABLE IF EXISTS jmi_gold_arbeitnow_slice.role_demand_monthly;
DROP TABLE IF EXISTS jmi_gold_arbeitnow_slice.location_demand_monthly;
DROP TABLE IF EXISTS jmi_gold_arbeitnow_slice.company_hiring_monthly;
DROP TABLE IF EXISTS jmi_gold_arbeitnow_slice.pipeline_run_summary;
DROP TABLE IF EXISTS jmi_gold_arbeitnow_slice.latest_run_metadata;
DROP DATABASE IF EXISTS jmi_gold_arbeitnow_slice;
