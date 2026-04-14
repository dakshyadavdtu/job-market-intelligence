-- Legacy flat-partition Silver table (ingest_date/run_id at silver/jobs/ root).
-- Active layout is silver/jobs/source=<slug>/ingest_date=.../run_id=... (jmi_silver_v2.*_merged + pipeline).
DROP TABLE IF EXISTS jmi_silver.jobs;
