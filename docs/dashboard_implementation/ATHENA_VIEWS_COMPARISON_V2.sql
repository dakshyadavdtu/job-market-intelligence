-- =============================================================================
-- ATHENA_VIEWS_COMPARISON_V2.sql — EU vs India comparison (dea final 6 minimal set)
-- Prerequisites: jmi_gold_v2.role_demand_monthly, jmi_gold_v2.skill_demand_monthly.
-- Engine: Athena engine 3 (Trino).
--
-- Retained for dashboards: aligned top-20 skill mix; benchmark row (skill-tag HHI inlined).
--
-- Honest scope:
--   - Posting volume in benchmark: SUM(job_count) from role_demand_monthly.
--   - Skill: skill_demand_monthly tag counts are NOT deduped per job; HHI / shares
--     are defined on tag-demand mass only.
--   - source column matches gold: arbeitnow | adzuna_in
-- =============================================================================

CREATE DATABASE IF NOT EXISTS jmi_analytics_v2;

-- -----------------------------------------------------------------------------
-- Aligned month + top-20 skills by combined tag mass; shares renormalized
-- within top-20 per source. Filtered to strict_intersection_latest_month.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_source_skill_mix_aligned_top20 AS
WITH month_bounds AS (
    SELECT
        date_format(date_add('month', -1, date_trunc('month', current_timestamp)), '%Y-%m') AS pm_min,
        date_format(date_trunc('month', current_timestamp), '%Y-%m') AS pm_max
),
month_latest_eu AS (
    SELECT r.posted_month, MAX(r.run_id) AS run_id
    FROM jmi_gold_v2.role_demand_monthly r
    CROSS JOIN month_bounds b
    WHERE r.source = 'arbeitnow'
      AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month
),
month_latest_ad AS (
    SELECT r.posted_month, MAX(r.run_id) AS run_id
    FROM jmi_gold_v2.role_demand_monthly r
    CROSS JOIN month_bounds b
    WHERE r.source = 'adzuna_in'
      AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month
),
intersection AS (
    SELECT e.posted_month
    FROM month_latest_eu e
    INNER JOIN month_latest_ad a ON e.posted_month = a.posted_month
),
strict_intersection_latest_month AS (
    SELECT MAX(i.posted_month) AS posted_month FROM intersection i
),
eu_skill_rows AS (
    SELECT
        s.skill,
        s.job_count,
        s.posted_month,
        s.run_id
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN month_latest_eu ml ON s.posted_month = ml.posted_month AND s.run_id = ml.run_id
    INNER JOIN strict_intersection_latest_month m ON s.posted_month = m.posted_month
    CROSS JOIN month_bounds b
    WHERE s.source = 'arbeitnow'
      AND s.posted_month BETWEEN b.pm_min AND b.pm_max
),
     eu_skill_tot AS (
         SELECT posted_month, run_id, SUM(job_count) AS tag_sum
         FROM eu_skill_rows
         GROUP BY posted_month, run_id
     ),
     eu_shares AS (
         SELECT
             CAST('arbeitnow' AS VARCHAR) AS source,
             e.skill,
             e.posted_month,
             e.run_id,
             e.job_count AS skill_tag_count,
             CAST(e.job_count AS DOUBLE) / CAST(NULLIF(t.tag_sum, 0) AS DOUBLE) AS share_within_source_skill_tags
         FROM eu_skill_rows e
         INNER JOIN eu_skill_tot t ON e.posted_month = t.posted_month AND e.run_id = t.run_id
     ),
     adzuna_skill_rows AS (
         SELECT
             s.skill,
             s.job_count,
             s.posted_month,
             s.run_id
         FROM jmi_gold_v2.skill_demand_monthly s
         INNER JOIN month_latest_ad ml ON s.posted_month = ml.posted_month AND s.run_id = ml.run_id
         INNER JOIN strict_intersection_latest_month m ON s.posted_month = m.posted_month
         CROSS JOIN month_bounds b
         WHERE s.source = 'adzuna_in'
           AND s.posted_month BETWEEN b.pm_min AND b.pm_max
     ),
     adzuna_skill_tot AS (
         SELECT posted_month, run_id, SUM(job_count) AS tag_sum
         FROM adzuna_skill_rows
         GROUP BY posted_month, run_id
     ),
     in_shares AS (
         SELECT
             CAST('adzuna_in' AS VARCHAR) AS source,
             i.skill,
             i.posted_month,
             i.run_id,
             i.job_count AS skill_tag_count,
             CAST(i.job_count AS DOUBLE) / CAST(NULLIF(t.tag_sum, 0) AS DOUBLE) AS share_within_source_skill_tags
         FROM adzuna_skill_rows i
         INNER JOIN adzuna_skill_tot t ON i.posted_month = t.posted_month AND i.run_id = t.run_id
     ),
     mix_base AS (
         SELECT * FROM eu_shares
         UNION ALL
         SELECT * FROM in_shares
     ),
     skill_totals AS (
         SELECT skill, SUM(skill_tag_count) AS total_tags
         FROM mix_base
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
         FROM mix_base m
         INNER JOIN top_skills t ON m.skill = t.skill
     )
SELECT
    source,
    skill,
    posted_month,
    run_id,
    skill_tag_count,
    CAST(skill_tag_count AS DOUBLE)
        / CAST(NULLIF(SUM(skill_tag_count) OVER (PARTITION BY source), 0) AS DOUBLE) AS share_within_source_skill_tags,
    CAST('strict_intersection_latest_month' AS VARCHAR) AS alignment_kind
FROM filt;

-- -----------------------------------------------------------------------------
-- Benchmark: strict intersection latest month — role postings and skill-tag HHI
-- (Skill-tag HHI CTEs inlined; no separate comparison_source_month_skill_tag_hhi view.)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_benchmark_aligned_month AS
WITH month_bounds AS (
    SELECT
        date_format(date_add('month', -1, date_trunc('month', current_timestamp)), '%Y-%m') AS pm_min,
        date_format(date_trunc('month', current_timestamp), '%Y-%m') AS pm_max
),
month_latest_eu AS (
    SELECT r.posted_month, MAX(r.run_id) AS run_id
    FROM jmi_gold_v2.role_demand_monthly r
    CROSS JOIN month_bounds b
    WHERE r.source = 'arbeitnow'
      AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month
),
month_latest_ad AS (
    SELECT r.posted_month, MAX(r.run_id) AS run_id
    FROM jmi_gold_v2.role_demand_monthly r
    CROSS JOIN month_bounds b
    WHERE r.source = 'adzuna_in'
      AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month
),
intersection AS (
    SELECT e.posted_month
    FROM month_latest_eu e
    INNER JOIN month_latest_ad a ON e.posted_month = a.posted_month
),
strict_intersection_latest_month AS (
    SELECT MAX(i.posted_month) AS posted_month FROM intersection i
),
skill_tag_hhi_base AS (
    SELECT
        CAST('arbeitnow' AS VARCHAR) AS source,
        s.skill,
        s.job_count,
        s.posted_month,
        s.run_id
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN month_latest_eu m ON s.posted_month = m.posted_month AND s.run_id = m.run_id
    CROSS JOIN month_bounds b
    WHERE s.source = 'arbeitnow'
      AND s.posted_month BETWEEN b.pm_min AND b.pm_max
    UNION ALL
    SELECT
        CAST('adzuna_in' AS VARCHAR) AS source,
        s.skill,
        s.job_count,
        s.posted_month,
        s.run_id
    FROM jmi_gold_v2.skill_demand_monthly s
    INNER JOIN month_latest_ad m ON s.posted_month = m.posted_month AND s.run_id = m.run_id
    CROSS JOIN month_bounds b
    WHERE s.source = 'adzuna_in'
      AND s.posted_month BETWEEN b.pm_min AND b.pm_max
),
skill_tag_hhi_tot AS (
    SELECT source, posted_month, run_id, SUM(job_count) AS tag_sum
    FROM skill_tag_hhi_base
    GROUP BY source, posted_month, run_id
),
skill_tag_hhi_sh AS (
    SELECT
        b.source,
        b.posted_month,
        b.run_id,
        CAST(b.job_count AS DOUBLE) / CAST(NULLIF(t.tag_sum, 0) AS DOUBLE) AS p
    FROM skill_tag_hhi_base b
    INNER JOIN skill_tag_hhi_tot t
        ON b.source = t.source
        AND b.posted_month = t.posted_month
        AND b.run_id = t.run_id
),
skill_tag_hhi AS (
    SELECT
        sh.source,
        sh.posted_month,
        sh.run_id,
        SUM(sh.p * sh.p) AS skill_tag_hhi,
        CAST(sh.posted_month IN (SELECT i.posted_month FROM intersection i) AS BOOLEAN) AS month_in_strict_intersection
    FROM skill_tag_hhi_sh sh
    GROUP BY sh.source, sh.posted_month, sh.run_id
),
role_eu AS (
    SELECT
        CAST('arbeitnow' AS VARCHAR) AS source,
        r.posted_month,
        r.run_id,
        SUM(r.job_count) AS total_role_postings
    FROM jmi_gold_v2.role_demand_monthly r
    INNER JOIN month_latest_eu ml ON r.posted_month = ml.posted_month AND r.run_id = ml.run_id
    INNER JOIN strict_intersection_latest_month a ON r.posted_month = a.posted_month
    CROSS JOIN month_bounds b
    WHERE r.source = 'arbeitnow'
      AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month, r.run_id
),
role_in AS (
    SELECT
        CAST('adzuna_in' AS VARCHAR) AS source,
        r.posted_month,
        r.run_id,
        SUM(r.job_count) AS total_role_postings
    FROM jmi_gold_v2.role_demand_monthly r
    INNER JOIN month_latest_ad ml ON r.posted_month = ml.posted_month AND r.run_id = ml.run_id
    INNER JOIN strict_intersection_latest_month a ON r.posted_month = a.posted_month
    CROSS JOIN month_bounds b
    WHERE r.source = 'adzuna_in'
      AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month, r.run_id
),
     roles AS (
         SELECT * FROM role_eu
         UNION ALL
         SELECT * FROM role_in
     )
SELECT
    r.source,
    r.posted_month AS aligned_posted_month,
    r.run_id,
    r.total_role_postings,
    h.skill_tag_hhi,
    CAST('strict_intersection_latest_month' AS VARCHAR) AS alignment_kind
FROM roles r
LEFT JOIN skill_tag_hhi h
    ON r.source = h.source
    AND r.posted_month = h.posted_month
    AND r.run_id = h.run_id
    AND h.month_in_strict_intersection = TRUE;
