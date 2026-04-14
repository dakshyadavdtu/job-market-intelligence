-- Drop legacy (non-v2) Glue/Athena databases only — does NOT delete S3 objects.
-- Run one statement per Athena query (Engine v3 does not accept multiple statements in one query).
-- Order: analytics first (views reference jmi_gold), then gold, then silver.

-- 1)
DROP DATABASE IF EXISTS jmi_analytics CASCADE;

-- 2)
DROP DATABASE IF EXISTS jmi_gold CASCADE;

-- 3)
DROP DATABASE IF EXISTS jmi_silver CASCADE;
