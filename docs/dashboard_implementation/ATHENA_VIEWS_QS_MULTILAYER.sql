-- =============================================================================
-- ATHENA_VIEWS_QS_MULTILAYER.sql — QuickSight helpers for multi-layer dashboard
-- Prerequisites: ATHENA_VIEWS.sql, ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql,
--                ATHENA_VIEWS_ADZUNA.sql (latest_pipeline_run_adzuna + role/skill views)
-- Do not alter jmi_gold.latest_run_metadata or jmi_analytics.latest_pipeline_run.
-- Engine: Athena engine 3 (Trino).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS jmi_analytics;

-- -----------------------------------------------------------------------------
-- LAYER 1 — EUROPE / ARBEITNOW: entity grain for histogram (employer posting counts)
-- QuickSight: Histogram on job_count (or bin in UI); Table for audit.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.europe_company_hiring_latest_grain AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run
)
SELECT
    c.company_name,
    c.job_count,
    c.source,
    c.posted_month,
    c.run_id
FROM jmi_gold.company_hiring_monthly c
INNER JOIN lr ON c.run_id = lr.run_id
WHERE c.source = 'arbeitnow'
  AND c.posted_month BETWEEN '2018-01' AND '2035-12';

-- -----------------------------------------------------------------------------
-- LAYER 2 — INDIA / ADZUNA: location × month for heat map (when multiple months exist)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.india_location_month_heatmap AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna
)
SELECT
    l.location AS location_label,
    l.posted_month,
    l.job_count,
    l.run_id,
    l.source
FROM jmi_gold.location_demand_monthly l
INNER JOIN lr ON l.run_id = lr.run_id
WHERE l.source = 'adzuna_in'
  AND l.posted_month BETWEEN '2018-01' AND '2035-12';

-- -----------------------------------------------------------------------------
-- LAYER 2 — INDIA / ADZUNA: city volume vs share of national volume (scatter / bubble)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.india_city_scatter_metrics AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna
),
base AS (
    SELECT
        l.location AS location_label,
        l.posted_month,
        l.job_count,
        l.run_id
    FROM jmi_gold.location_demand_monthly l
    INNER JOIN lr ON l.run_id = lr.run_id
    WHERE l.source = 'adzuna_in'
      AND l.posted_month BETWEEN '2018-01' AND '2035-12'
),
tot AS (
    SELECT run_id, posted_month, SUM(job_count) AS national_total
    FROM base
    GROUP BY run_id, posted_month
)
SELECT
    b.location_label,
    b.posted_month,
    b.job_count AS city_job_count,
    b.run_id,
    CAST(b.job_count AS DOUBLE) / CAST(NULLIF(t.national_total, 0) AS DOUBLE) AS city_share_of_national
FROM base b
INNER JOIN tot t ON b.run_id = t.run_id AND b.posted_month = t.posted_month;

-- -----------------------------------------------------------------------------
-- LAYER 2 — INDIA / ADZUNA: box-plot friendly grain (skill rows)
-- Reuses existing wide view; explicit alias for QS dataset naming.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.india_skill_job_count_boxplot_grain AS
SELECT
    skill,
    job_count,
    source,
    posted_month,
    run_id
FROM jmi_analytics.skill_demand_monthly_adzuna_latest;

-- -----------------------------------------------------------------------------
-- LAYER 3 — COMPARISON: total postings by region label and month (benchmark trend)
-- EU = latest Arbeitnow pipeline run; IN = latest Adzuna pipeline run (separate run_ids).
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.comparison_region_month_totals AS
SELECT
    'EU' AS region_label,
    r.posted_month,
    r.run_id,
    SUM(r.job_count) AS total_postings
FROM jmi_gold.role_demand_monthly r
INNER JOIN (SELECT run_id FROM jmi_analytics.latest_pipeline_run) lr ON r.run_id = lr.run_id
WHERE r.source = 'arbeitnow'
  AND r.posted_month BETWEEN '2018-01' AND '2035-12'
GROUP BY r.posted_month, r.run_id

UNION ALL

SELECT
    'IN' AS region_label,
    r.posted_month,
    r.run_id,
    SUM(r.job_count) AS total_postings
FROM jmi_gold.role_demand_monthly r
INNER JOIN (SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna) lr ON r.run_id = lr.run_id
WHERE r.source = 'adzuna_in'
  AND r.posted_month BETWEEN '2018-01' AND '2035-12'
GROUP BY r.posted_month, r.run_id;

-- -----------------------------------------------------------------------------
-- LAYER 3 — COMPARISON: skill tag shares within region (for 100% stacked / mix charts)
-- Top skills by raw count per region run; share = skill_count / region_total_skill_tag_sum
-- Note: skill tags are not deduped across jobs; interpret as tag-demand mix, not job counts.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.comparison_region_skill_mix AS
WITH eu AS (
    SELECT
        s.skill,
        s.job_count,
        s.posted_month,
        s.run_id
    FROM jmi_gold.skill_demand_monthly s
    INNER JOIN (SELECT run_id FROM jmi_analytics.latest_pipeline_run) lr ON s.run_id = lr.run_id
    WHERE s.source = 'arbeitnow'
      AND s.posted_month BETWEEN '2018-01' AND '2035-12'
),
eu_tot AS (
    SELECT posted_month, run_id, SUM(job_count) AS tag_sum
    FROM eu
    GROUP BY posted_month, run_id
),
eu_shares AS (
    SELECT
        'EU' AS region_label,
        e.skill,
        e.posted_month,
        e.run_id,
        e.job_count AS skill_tag_count,
        CAST(e.job_count AS DOUBLE) / CAST(NULLIF(t.tag_sum, 0) AS DOUBLE) AS share_within_region_skill_tags
    FROM eu e
    INNER JOIN eu_tot t ON e.posted_month = t.posted_month AND e.run_id = t.run_id
),
adzuna_skills AS (
    SELECT
        s.skill,
        s.job_count,
        s.posted_month,
        s.run_id
    FROM jmi_gold.skill_demand_monthly s
    INNER JOIN (SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna) lr ON s.run_id = lr.run_id
    WHERE s.source = 'adzuna_in'
      AND s.posted_month BETWEEN '2018-01' AND '2035-12'
),
adzuna_skill_tot AS (
    SELECT posted_month, run_id, SUM(job_count) AS tag_sum
    FROM adzuna_skills
    GROUP BY posted_month, run_id
),
in_shares AS (
    SELECT
        'IN' AS region_label,
        i.skill,
        i.posted_month,
        i.run_id,
        i.job_count AS skill_tag_count,
        CAST(i.job_count AS DOUBLE) / CAST(NULLIF(t.tag_sum, 0) AS DOUBLE) AS share_within_region_skill_tags
    FROM adzuna_skills i
    INNER JOIN adzuna_skill_tot t ON i.posted_month = t.posted_month AND i.run_id = t.run_id
)
SELECT * FROM eu_shares
UNION ALL
SELECT * FROM in_shares;

-- =============================================================================
-- NOTES (QuickSight)
-- -----------------------------------------------------------------------------
-- Gauge (EU): bind to jmi_analytics.sheet1_kpis — e.g. location_hhi or top3_location_share; set min/max in QS.
-- Sankey / Radar: not materialized here — add only if you introduce honest edge-list or normalized pivot views.
-- =============================================================================
