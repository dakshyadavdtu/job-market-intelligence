-- =============================================================================
-- ATHENA_VIEWS.sql — Frozen implementation SQL for QuickSight datasets
-- =============================================================================
-- Prerequisites:
--   - Database `jmi_gold` exists with external tables:
--       role_demand_monthly, location_demand_monthly, company_hiring_monthly,
--       skill_demand_monthly, pipeline_run_summary, latest_run_metadata
--   - Partition columns: ingest_month, run_id (Hive-style partitions on S3)
--   - Gold DDL under infra/aws/athena/ddl_gold_*.sql uses partition projection
--     (including pipeline_run_summary) so new S3 prefixes resolve without MSCK.
--   - Partition projection requires BOTH partition keys in predicates: `run_id` (enum in
--     ddl_gold_*.sql; append new run_ids in Glue) AND `ingest_month` (date projection). Every view below includes
--     `ingest_month BETWEEN '2018-01' AND '2035-12'` — MUST match
--     `projection.ingest_month.range` in those DDL files (alter both if you widen range).
--   - Column names match repo DDL (bronze_ingest_date, bronze_run_id in body rows)
--
-- Analytics database for views (keeps gold tables in `jmi_gold` unchanged).
-- Use CREATE DATABASE — Athena/Glue use this; CREATE SCHEMA may fail in some workgroups.
--
-- Latest run: jmi_gold.latest_run_metadata holds one row (run_id), overwritten each Gold
-- run by transform_gold.py. Views filter fact/summary data to that run_id.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS jmi_analytics;

-- -----------------------------------------------------------------------------
-- 0) latest_pipeline_run — Single-row helper (newest Gold pipeline run_id)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.latest_pipeline_run AS
SELECT run_id FROM jmi_gold.latest_run_metadata LIMIT 1;

-- -----------------------------------------------------------------------------
-- 0b) Raw-grain helpers — Same columns as jmi_gold tables, latest run_id only
--      (use for QuickSight datasets that previously pointed at jmi_gold.* directly.)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.skill_demand_monthly_latest AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run
)
SELECT
    s.skill,
    s.job_count,
    s.source,
    s.bronze_ingest_date,
    s.bronze_run_id,
    s.ingest_month,
    s.run_id
FROM jmi_gold.skill_demand_monthly s
INNER JOIN lr ON s.run_id = lr.run_id
WHERE s.ingest_month BETWEEN '2018-01' AND '2035-12';

CREATE OR REPLACE VIEW jmi_analytics.pipeline_run_summary_latest AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run
)
SELECT
    p.source,
    p.bronze_ingest_date,
    p.bronze_run_id,
    p.skill_row_count,
    p.role_row_count,
    p.location_row_count,
    p.company_row_count,
    p.status,
    p.ingest_month,
    p.run_id
FROM jmi_gold.pipeline_run_summary p
INNER JOIN lr ON p.run_id = lr.run_id
WHERE p.ingest_month BETWEEN '2018-01' AND '2035-12';

-- -----------------------------------------------------------------------------
-- 1) sheet1_kpis — One row per (ingest_month, run_id) with all six KPI fields
--     (restricted to latest pipeline run only)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.sheet1_kpis AS
WITH
lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run
),
role_totals AS (
    SELECT
        r.ingest_month,
        r.run_id,
        SUM(r.job_count) AS total_postings,
        MAX(r.job_count) AS max_role_job_count
    FROM jmi_gold.role_demand_monthly r
    INNER JOIN lr ON r.run_id = lr.run_id
    WHERE r.ingest_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY r.ingest_month, r.run_id
),
loc_totals AS (
    SELECT
        l.ingest_month,
        l.run_id,
        SUM(l.job_count) AS located_postings
    FROM jmi_gold.location_demand_monthly l
    INNER JOIN lr ON l.run_id = lr.run_id
    WHERE l.ingest_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY l.ingest_month, l.run_id
),
loc_top3 AS (
    SELECT
        ingest_month,
        run_id,
        SUM(job_count) AS top3_location_job_sum
    FROM (
        SELECT
            l.ingest_month,
            l.run_id,
            l.job_count,
            ROW_NUMBER() OVER (
                PARTITION BY l.ingest_month, l.run_id
                ORDER BY l.job_count DESC, l.location ASC
            ) AS rn
        FROM jmi_gold.location_demand_monthly l
        INNER JOIN lr ON l.run_id = lr.run_id
        WHERE l.ingest_month BETWEEN '2018-01' AND '2035-12'
    ) x
    WHERE rn <= 3
    GROUP BY ingest_month, run_id
),
loc_hhi_calc AS (
    SELECT
        l.ingest_month,
        l.run_id,
        SUM(
            POWER(
                CAST(l.job_count AS DOUBLE) / CAST(lt.located_postings AS DOUBLE),
                2
            )
        ) AS location_hhi
    FROM jmi_gold.location_demand_monthly l
    INNER JOIN lr ON l.run_id = lr.run_id
    INNER JOIN loc_totals lt
        ON l.ingest_month = lt.ingest_month
        AND l.run_id = lt.run_id
    WHERE lt.located_postings > 0
        AND l.ingest_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY l.ingest_month, l.run_id
),
comp_totals AS (
    SELECT
        c.ingest_month,
        c.run_id,
        SUM(c.job_count) AS company_postings_sum
    FROM jmi_gold.company_hiring_monthly c
    INNER JOIN lr ON c.run_id = lr.run_id
    WHERE c.ingest_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY c.ingest_month, c.run_id
),
comp_hhi_calc AS (
    SELECT
        c.ingest_month,
        c.run_id,
        SUM(
            POWER(
                CAST(c.job_count AS DOUBLE) / CAST(ct.company_postings_sum AS DOUBLE),
                2
            )
        ) AS company_hhi
    FROM jmi_gold.company_hiring_monthly c
    INNER JOIN lr ON c.run_id = lr.run_id
    INNER JOIN comp_totals ct
        ON c.ingest_month = ct.ingest_month
        AND c.run_id = ct.run_id
    WHERE ct.company_postings_sum > 0
        AND c.ingest_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY c.ingest_month, c.run_id
)
SELECT
    r.ingest_month,
    r.run_id,
    r.total_postings,
    COALESCE(l.located_postings, CAST(0 AS BIGINT)) AS located_postings,
    -- Top-3 share: sum of top 3 location job_count / total located postings
    CASE
        WHEN COALESCE(l.located_postings, 0) > 0
            THEN CAST(COALESCE(t3.top3_location_job_sum, 0) AS DOUBLE)
                / CAST(l.located_postings AS DOUBLE)
        ELSE NULL
    END AS top3_location_share,
    lh.location_hhi AS location_hhi,
    ch.company_hhi AS company_hhi,
    -- Top-1 role share: max role job_count / total_postings
    CASE
        WHEN r.total_postings > 0
            THEN CAST(r.max_role_job_count AS DOUBLE) / CAST(r.total_postings AS DOUBLE)
        ELSE NULL
    END AS top1_role_share
FROM role_totals r
LEFT JOIN loc_totals l
    ON r.ingest_month = l.ingest_month AND r.run_id = l.run_id
LEFT JOIN loc_top3 t3
    ON r.ingest_month = t3.ingest_month AND r.run_id = t3.run_id
LEFT JOIN loc_hhi_calc lh
    ON r.ingest_month = lh.ingest_month AND r.run_id = lh.run_id
LEFT JOIN comp_hhi_calc ch
    ON r.ingest_month = ch.ingest_month AND r.run_id = ch.run_id;

-- -----------------------------------------------------------------------------
-- 2) location_top15_other — Top 15 locations + Other (for treemap + table)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.location_top15_other AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run
),
ranked AS (
    SELECT
        l.ingest_month,
        l.run_id,
        l.location,
        l.job_count,
        ROW_NUMBER() OVER (
            PARTITION BY l.ingest_month, l.run_id
            ORDER BY l.job_count DESC, l.location ASC
        ) AS rn
    FROM jmi_gold.location_demand_monthly l
    INNER JOIN lr ON l.run_id = lr.run_id
    WHERE l.ingest_month BETWEEN '2018-01' AND '2035-12'
),
rolled AS (
    SELECT
        ingest_month,
        run_id,
        CASE
            WHEN rn <= 15 THEN location
            ELSE 'Other'
        END AS location_label,
        SUM(job_count) AS job_count
    FROM ranked
    GROUP BY
        ingest_month,
        run_id,
        CASE
            WHEN rn <= 15 THEN location
            ELSE 'Other'
        END
)
SELECT
    ingest_month,
    run_id,
    location_label,
    job_count
FROM rolled
WHERE job_count > 0;

-- -----------------------------------------------------------------------------
-- 3) company_top12_other — Top 12 companies + Other
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.company_top12_other AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run
),
ranked AS (
    SELECT
        c.ingest_month,
        c.run_id,
        c.company_name,
        c.job_count,
        ROW_NUMBER() OVER (
            PARTITION BY c.ingest_month, c.run_id
            ORDER BY c.job_count DESC, c.company_name ASC
        ) AS rn
    FROM jmi_gold.company_hiring_monthly c
    INNER JOIN lr ON c.run_id = lr.run_id
    WHERE c.ingest_month BETWEEN '2018-01' AND '2035-12'
),
rolled AS (
    SELECT
        ingest_month,
        run_id,
        CASE
            WHEN rn <= 12 THEN company_name
            ELSE 'Other'
        END AS company_label,
        SUM(job_count) AS job_count
    FROM ranked
    GROUP BY
        ingest_month,
        run_id,
        CASE
            WHEN rn <= 12 THEN company_name
            ELSE 'Other'
        END
)
SELECT
    ingest_month,
    run_id,
    company_label,
    job_count
FROM rolled
WHERE job_count > 0;

-- -----------------------------------------------------------------------------
-- 4) role_pareto — Full role set with pareto_rank, share, cumulative %
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.role_pareto AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run
),
totals AS (
    SELECT
        r.ingest_month,
        r.run_id,
        SUM(r.job_count) AS total_jobs
    FROM jmi_gold.role_demand_monthly r
    INNER JOIN lr ON r.run_id = lr.run_id
    WHERE r.ingest_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY r.ingest_month, r.run_id
)
SELECT
    r.ingest_month,
    r.run_id,
    r.role,
    r.job_count,
    ROW_NUMBER() OVER (
        PARTITION BY r.ingest_month, r.run_id
        ORDER BY r.job_count DESC, r.role ASC
    ) AS pareto_rank,
    CASE
        WHEN t.total_jobs > 0
            THEN CAST(r.job_count AS DOUBLE) / CAST(t.total_jobs AS DOUBLE)
        ELSE NULL
    END AS share_of_total,
    CASE
        WHEN t.total_jobs > 0
            THEN 100.0 * SUM(r.job_count) OVER (
                PARTITION BY r.ingest_month, r.run_id
                ORDER BY r.job_count DESC, r.role ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / CAST(t.total_jobs AS DOUBLE)
        ELSE NULL
    END AS cumulative_job_pct
FROM jmi_gold.role_demand_monthly r
INNER JOIN lr ON r.run_id = lr.run_id
INNER JOIN totals t
    ON r.ingest_month = t.ingest_month
    AND r.run_id = t.run_id
WHERE r.ingest_month BETWEEN '2018-01' AND '2035-12';

-- -----------------------------------------------------------------------------
-- 5) role_top20 — Support table for Sheet 1 (top 20 by postings)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.role_top20 AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run
),
ranked AS (
    SELECT
        r.ingest_month,
        r.run_id,
        r.role,
        r.job_count,
        ROW_NUMBER() OVER (
            PARTITION BY r.ingest_month, r.run_id
            ORDER BY r.job_count DESC, r.role ASC
        ) AS pareto_rank
    FROM jmi_gold.role_demand_monthly r
    INNER JOIN lr ON r.run_id = lr.run_id
    WHERE r.ingest_month BETWEEN '2018-01' AND '2035-12'
)
SELECT
    ingest_month,
    run_id,
    role,
    job_count,
    pareto_rank
FROM ranked
WHERE pareto_rank <= 20;

-- =============================================================================
-- NOTES (schema assumptions)
-- =============================================================================
-- 1) If your Glue/Athena table names differ, replace `jmi_gold` prefix only.
-- 2) Gold tables use partition projection in repo DDL; queries must filter both
--    `run_id` (via latest_pipeline_run join) and `ingest_month` within the DDL
--    projection range — otherwise Athena may scan no paths. New S3 prefixes under
--    the configured month/run_id template are visible without MSCK REPAIR.
-- 3) latest_run_metadata is a single overwritten Parquet file; no MSCK. Run the Gold
--    transform so the file exists before relying on latest_pipeline_run.
-- 4) `location_hhi` / `company_hhi`: when located_postings = 0 or
--    company_postings_sum = 0, the INNER JOIN in loc_hhi_calc / comp_hhi_calc
--    yields no row; sheet1_kpis LEFT JOINs those CTEs so KPI columns are NULL.
--    Single-location slice still yields HHI = 1.0 from the HHI formula.
-- 5) QuickSight: import views from `jmi_analytics` with Direct Query or SPICE;
--    refresh SPICE after new runs.
-- 6) Optional Sheet 1 quality views (role families, cleaner companies):
--    see docs/dashboard_implementation/ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql
-- =============================================================================
