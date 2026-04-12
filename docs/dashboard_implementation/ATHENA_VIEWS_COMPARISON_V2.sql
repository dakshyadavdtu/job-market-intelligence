-- =============================================================================
-- ATHENA_VIEWS_COMPARISON_V2.sql — Honest benchmark helpers (jmi_analytics_v2 only)
-- Prerequisites: jmi_gold_v2 + jmi_analytics_v2.latest_pipeline_run,
--                jmi_analytics_v2.latest_pipeline_run_adzuna
-- Engine: Athena engine 3 (Trino).
--
-- Honest scope:
--   - Posting volume: SUM(job_count) from role_demand_monthly (role-title buckets).
--   - Skill: skill_demand_monthly tag counts are NOT deduped per job; HHI / shares
--     are defined on tag-demand mass only.
--   - source column matches gold: arbeitnow | adzuna_in (no EU/IN aliases).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS jmi_analytics_v2;

-- -----------------------------------------------------------------------------
-- 1) Total postings by gold source and calendar month (latest run per source)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_source_month_totals AS
SELECT
    CAST('arbeitnow' AS VARCHAR) AS source,
    r.ingest_month,
    r.run_id,
    SUM(r.job_count) AS total_postings
FROM jmi_gold_v2.role_demand_monthly r
INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run) lr ON r.run_id = lr.run_id
WHERE r.source = 'arbeitnow'
  AND r.ingest_month BETWEEN '2018-01' AND '2035-12'
GROUP BY r.ingest_month, r.run_id

UNION ALL

SELECT
    CAST('adzuna_in' AS VARCHAR) AS source,
    r.ingest_month,
    r.run_id,
    SUM(r.job_count) AS total_postings
FROM jmi_gold_v2.role_demand_monthly r
INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run_adzuna) lr ON r.run_id = lr.run_id
WHERE r.source = 'adzuna_in'
  AND r.ingest_month BETWEEN '2018-01' AND '2035-12'
GROUP BY r.ingest_month, r.run_id;

-- -----------------------------------------------------------------------------
-- 2) Skill tag shares (full grain): one row per source × month × skill
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_source_skill_mix AS
WITH eu AS (
    SELECT
        s.skill,
        s.job_count,
        s.ingest_month,
        s.run_id
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run) lr ON s.run_id = lr.run_id
    WHERE s.source = 'arbeitnow'
      AND s.ingest_month BETWEEN '2018-01' AND '2035-12'
),
eu_tot AS (
    SELECT ingest_month, run_id, SUM(job_count) AS tag_sum
    FROM eu
    GROUP BY ingest_month, run_id
),
eu_shares AS (
    SELECT
        CAST('arbeitnow' AS VARCHAR) AS source,
        e.skill,
        e.ingest_month,
        e.run_id,
        e.job_count AS skill_tag_count,
        CAST(e.job_count AS DOUBLE) / CAST(NULLIF(t.tag_sum, 0) AS DOUBLE) AS share_within_source_skill_tags
    FROM eu e
    INNER JOIN eu_tot t ON e.ingest_month = t.ingest_month AND e.run_id = t.run_id
),
adzuna_skills AS (
    SELECT
        s.skill,
        s.job_count,
        s.ingest_month,
        s.run_id
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run_adzuna) lr ON s.run_id = lr.run_id
    WHERE s.source = 'adzuna_in'
      AND s.ingest_month BETWEEN '2018-01' AND '2035-12'
),
adzuna_skill_tot AS (
    SELECT ingest_month, run_id, SUM(job_count) AS tag_sum
    FROM adzuna_skills
    GROUP BY ingest_month, run_id
),
in_shares AS (
    SELECT
        CAST('adzuna_in' AS VARCHAR) AS source,
        i.skill,
        i.ingest_month,
        i.run_id,
        i.job_count AS skill_tag_count,
        CAST(i.job_count AS DOUBLE) / CAST(NULLIF(t.tag_sum, 0) AS DOUBLE) AS share_within_source_skill_tags
    FROM adzuna_skills i
    INNER JOIN adzuna_skill_tot t ON i.ingest_month = t.ingest_month AND i.run_id = t.run_id
)
SELECT * FROM eu_shares
UNION ALL
SELECT * FROM in_shares;

-- -----------------------------------------------------------------------------
-- 3) Skill-tag concentration (HHI) per source × month (on tag-demand distribution)
--    HHI = sum_i (share_i^2) where share_i is tag_count_i / sum(tags) for that month.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_source_month_skill_tag_hhi AS
WITH base AS (
    SELECT
        CAST('arbeitnow' AS VARCHAR) AS source,
        s.skill,
        s.job_count,
        s.ingest_month,
        s.run_id
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run) lr ON s.run_id = lr.run_id
    WHERE s.source = 'arbeitnow'
      AND s.ingest_month BETWEEN '2018-01' AND '2035-12'
    UNION ALL
    SELECT
        CAST('adzuna_in' AS VARCHAR) AS source,
        s.skill,
        s.job_count,
        s.ingest_month,
        s.run_id
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run_adzuna) lr ON s.run_id = lr.run_id
    WHERE s.source = 'adzuna_in'
      AND s.ingest_month BETWEEN '2018-01' AND '2035-12'
),
tot AS (
    SELECT source, ingest_month, run_id, SUM(job_count) AS tag_sum
    FROM base
    GROUP BY source, ingest_month, run_id
),
sh AS (
    SELECT
        b.source,
        b.ingest_month,
        b.run_id,
        CAST(b.job_count AS DOUBLE) / CAST(NULLIF(t.tag_sum, 0) AS DOUBLE) AS p
    FROM base b
    INNER JOIN tot t
        ON b.source = t.source
        AND b.ingest_month = t.ingest_month
        AND b.run_id = t.run_id
)
SELECT
    source,
    ingest_month,
    run_id,
    SUM(p * p) AS skill_tag_hhi
FROM sh
GROUP BY source, ingest_month, run_id;

-- -----------------------------------------------------------------------------
-- 4) Aligned month + top-20 skills by combined tag mass; shares renormalized
--    within top-20 per source (partial mix; sums to 100% within slice).
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_source_skill_mix_aligned_top20 AS
WITH eu_months AS (
    SELECT DISTINCT ingest_month
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run) lr ON s.run_id = lr.run_id
    WHERE s.source = 'arbeitnow'
      AND s.ingest_month BETWEEN '2018-01' AND '2035-12'
),
in_months AS (
    SELECT DISTINCT ingest_month
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run_adzuna) lr ON s.run_id = lr.run_id
    WHERE s.source = 'adzuna_in'
      AND s.ingest_month BETWEEN '2018-01' AND '2035-12'
),
aligned AS (
    SELECT MAX(e.ingest_month) AS ingest_month
    FROM eu_months e
    INNER JOIN in_months i ON e.ingest_month = i.ingest_month
),
mix AS (
    SELECT m.*
    FROM jmi_analytics_v2.comparison_source_skill_mix m
    CROSS JOIN aligned a
    WHERE m.ingest_month = a.ingest_month
),
skill_totals AS (
    SELECT skill, SUM(skill_tag_count) AS total_tags
    FROM mix
    GROUP BY skill
),
top_skills AS (
    SELECT skill
    FROM skill_totals
    ORDER BY total_tags DESC
    LIMIT 20
),
filt AS (
    SELECT m.*
    FROM mix m
    INNER JOIN top_skills t ON m.skill = t.skill
)
SELECT
    source,
    skill,
    ingest_month,
    run_id,
    skill_tag_count,
    CAST(skill_tag_count AS DOUBLE)
        / CAST(NULLIF(SUM(skill_tag_count) OVER (PARTITION BY source), 0) AS DOUBLE) AS share_within_source_skill_tags
FROM filt;

-- -----------------------------------------------------------------------------
-- 5) Benchmark row: latest calendar month common to both sources — role postings
--    and skill-tag HHI side-by-side (one row per source).
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_benchmark_aligned_month AS
WITH eu_months AS (
    SELECT DISTINCT ingest_month
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run) lr ON s.run_id = lr.run_id
    WHERE s.source = 'arbeitnow'
      AND s.ingest_month BETWEEN '2018-01' AND '2035-12'
),
in_months AS (
    SELECT DISTINCT ingest_month
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run_adzuna) lr ON s.run_id = lr.run_id
    WHERE s.source = 'adzuna_in'
      AND s.ingest_month BETWEEN '2018-01' AND '2035-12'
),
aligned AS (
    SELECT MAX(e.ingest_month) AS ingest_month
    FROM eu_months e
    INNER JOIN in_months i ON e.ingest_month = i.ingest_month
),
role_eu AS (
    SELECT
        CAST('arbeitnow' AS VARCHAR) AS source,
        r.ingest_month,
        r.run_id,
        SUM(r.job_count) AS total_role_postings
    FROM jmi_gold_v2.role_demand_monthly r
    INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run) lr ON r.run_id = lr.run_id
    INNER JOIN aligned a ON r.ingest_month = a.ingest_month
    WHERE r.source = 'arbeitnow'
      AND r.ingest_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY r.ingest_month, r.run_id
),
role_in AS (
    SELECT
        CAST('adzuna_in' AS VARCHAR) AS source,
        r.ingest_month,
        r.run_id,
        SUM(r.job_count) AS total_role_postings
    FROM jmi_gold_v2.role_demand_monthly r
    INNER JOIN (SELECT run_id FROM jmi_analytics_v2.latest_pipeline_run_adzuna) lr ON r.run_id = lr.run_id
    INNER JOIN aligned a ON r.ingest_month = a.ingest_month
    WHERE r.source = 'adzuna_in'
      AND r.ingest_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY r.ingest_month, r.run_id
),
roles AS (
    SELECT * FROM role_eu
    UNION ALL
    SELECT * FROM role_in
)
SELECT
    r.source,
    r.ingest_month AS aligned_ingest_month,
    r.run_id,
    r.total_role_postings,
    h.skill_tag_hhi
FROM roles r
LEFT JOIN jmi_analytics_v2.comparison_source_month_skill_tag_hhi h
    ON r.source = h.source
    AND r.ingest_month = h.ingest_month
    AND r.run_id = h.run_id;

-- -----------------------------------------------------------------------------
-- Legacy cleanup (optional): older phase used comparison_region_* and region_label.
-- Run once if those views still exist in your catalog:
--   DROP VIEW IF EXISTS jmi_analytics_v2.comparison_region_skill_mix_aligned_top20;
--   DROP VIEW IF EXISTS jmi_analytics_v2.comparison_region_skill_mix;
--   DROP VIEW IF EXISTS jmi_analytics_v2.comparison_region_month_totals;
-- -----------------------------------------------------------------------------
