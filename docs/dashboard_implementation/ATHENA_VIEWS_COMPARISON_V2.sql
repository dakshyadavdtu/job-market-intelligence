-- =============================================================================
-- ATHENA_VIEWS_COMPARISON_V2.sql — Honest benchmark helpers (jmi_analytics_v2 only)
-- Prerequisites: jmi_gold_v2.role_demand_monthly, jmi_gold_v2.skill_demand_monthly.
-- Engine: Athena engine 3 (Trino).
--
-- Time-window policy (read before using):
--   - Latest EU run (arbeitnow) and latest India run (adzuna_in) are INDEPENDENT.
--     Month coverage differs by source; a UNION of monthly totals is EXPLORATORY only.
--   - Strict source-to-source comparison uses calendar months in the INTERSECTION of
--     posted_month sets where BOTH sources have Gold for that month in the rolling
--     previous+current UTC month, using MAX(run_id) per posted_month per source (not
--     only the single latest_run_metadata_arbeitnow pointer).
--   - "Aligned" / benchmark views use MAX(intersection posted_month) = latest common
--     calendar month — NOT March unless both sources actually have that March.
--   - March-only strict comparability: inspect comparison_time_window_policy.march_strict_comparable_both_sources
--     (and intersection months) — do not assume March coverage exists for both sources.
--   - Multi-year / "last N years" stories: use comparison_time_window_policy.observed_month_span_inclusive
--     and ten_year_window_claim_valid; do not imply a full decade if coverage is shorter.
--
-- Honest scope:
--   - Posting volume: SUM(job_count) from role_demand_monthly (role-title buckets).
--   - Skill: skill_demand_monthly tag counts are NOT deduped per job; HHI / shares
--     are defined on tag-demand mass only.
--   - source column matches gold: arbeitnow | adzuna_in (no EU/IN aliases).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS jmi_analytics_v2;

-- -----------------------------------------------------------------------------
-- 0) Distinct posted_month values in the strict intersection (per-month MAX(run_id) in live window)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_strict_intersection_months AS
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
)
SELECT e.posted_month
FROM month_latest_eu e
INNER JOIN month_latest_ad a ON e.posted_month = a.posted_month;

-- -----------------------------------------------------------------------------
-- 0b) Single-row policy row for dashboards (no guessing; inspect columns in QS)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_time_window_policy AS
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
eu_m AS (SELECT DISTINCT posted_month FROM month_latest_eu),
ad_m AS (SELECT DISTINCT posted_month FROM month_latest_ad),
intersection AS (
    SELECT e.posted_month
    FROM month_latest_eu e
    INNER JOIN month_latest_ad a ON e.posted_month = a.posted_month
),
bounds AS (
    SELECT b.pm_min AS min_posted_month, b.pm_max AS max_posted_month
    FROM month_bounds b
)
SELECT
    (SELECT MAX(i.posted_month) FROM intersection i) AS strict_intersection_latest_month,
    CAST((SELECT COUNT(*) FROM intersection i) AS BIGINT) AS strict_intersection_month_count,
    CAST(
        EXISTS (SELECT 1 FROM intersection i WHERE i.posted_month LIKE '%-03') AS BOOLEAN
    ) AS march_strict_comparable_both_sources,
    b.min_posted_month AS exploratory_union_min_posted_month,
    b.max_posted_month AS exploratory_union_max_posted_month,
    CAST(
        date_diff(
            'month',
            date_parse(b.min_posted_month, '%Y-%m'),
            date_parse(b.max_posted_month, '%Y-%m')
        ) AS BIGINT
    ) AS observed_month_span_inclusive,
    CAST(
        (date_diff(
            'month',
            date_parse(b.min_posted_month, '%Y-%m'),
            date_parse(b.max_posted_month, '%Y-%m')
        ) >= 120)
        AS BOOLEAN
    ) AS ten_year_window_claim_valid
FROM bounds b;

-- -----------------------------------------------------------------------------
-- 1) Total postings by gold source and calendar month (MAX(run_id) per posted_month
--     per source within the rolling previous+current UTC month window).
--     layer_scope = exploratory: union of months may differ by source; use month_in_strict_intersection
--     to filter to apples-to-apples months only.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_source_month_totals AS
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
ar AS (
    SELECT
        CAST('arbeitnow' AS VARCHAR) AS source,
        r.posted_month,
        r.run_id,
        SUM(r.job_count) AS total_postings
    FROM jmi_gold_v2.role_demand_monthly r
    INNER JOIN month_latest_eu m ON r.posted_month = m.posted_month AND r.run_id = m.run_id
    CROSS JOIN month_bounds b
    WHERE r.source = 'arbeitnow'
      AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month, r.run_id
),
ad AS (
    SELECT
        CAST('adzuna_in' AS VARCHAR) AS source,
        r.posted_month,
        r.run_id,
        SUM(r.job_count) AS total_postings
    FROM jmi_gold_v2.role_demand_monthly r
    INNER JOIN month_latest_ad m ON r.posted_month = m.posted_month AND r.run_id = m.run_id
    CROSS JOIN month_bounds b
    WHERE r.source = 'adzuna_in'
      AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month, r.run_id
)
SELECT
    ar.source,
    ar.posted_month,
    ar.run_id,
    ar.total_postings,
    CAST('exploratory_latest_runs_uneven' AS VARCHAR) AS layer_scope,
    CAST(ar.posted_month IN (SELECT i.posted_month FROM intersection i) AS BOOLEAN) AS month_in_strict_intersection
FROM ar
UNION ALL
SELECT
    ad.source,
    ad.posted_month,
    ad.run_id,
    ad.total_postings,
    CAST('exploratory_latest_runs_uneven' AS VARCHAR) AS layer_scope,
    CAST(ad.posted_month IN (SELECT i.posted_month FROM intersection i) AS BOOLEAN) AS month_in_strict_intersection
FROM ad;

-- -----------------------------------------------------------------------------
-- 1b) Strict intersection only — same thickness per source (one row per source per common month)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_strict_intersection_month_totals AS
SELECT
    source,
    posted_month,
    run_id,
    total_postings,
    CAST('strict_intersection' AS VARCHAR) AS layer_scope
FROM jmi_analytics_v2.comparison_source_month_totals
WHERE month_in_strict_intersection = TRUE;

-- -----------------------------------------------------------------------------
-- 2) Skill-tag concentration (HHI) per source × month (on tag-demand distribution)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.comparison_source_month_skill_tag_hhi AS
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
base AS (
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
     tot AS (
         SELECT source, posted_month, run_id, SUM(job_count) AS tag_sum
         FROM base
         GROUP BY source, posted_month, run_id
     ),
     sh AS (
         SELECT
             b.source,
             b.posted_month,
             b.run_id,
             CAST(b.job_count AS DOUBLE) / CAST(NULLIF(t.tag_sum, 0) AS DOUBLE) AS p
         FROM base b
         INNER JOIN tot t
             ON b.source = t.source
             AND b.posted_month = t.posted_month
             AND b.run_id = t.run_id
     )
SELECT
    sh.source,
    sh.posted_month,
    sh.run_id,
    SUM(sh.p * sh.p) AS skill_tag_hhi,
    CAST(sh.posted_month IN (SELECT i.posted_month FROM intersection i) AS BOOLEAN) AS month_in_strict_intersection
FROM sh
GROUP BY sh.source, sh.posted_month, sh.run_id;

-- -----------------------------------------------------------------------------
-- 3) Aligned month + top-20 skills by combined tag mass; shares renormalized
--     within top-20 per source (partial mix; sums to 100% within slice).
--     Filtered to strict_intersection_latest_month = MAX(month in intersection).
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
-- 4) Benchmark row: strict intersection latest month — role postings and skill-tag HHI
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
LEFT JOIN jmi_analytics_v2.comparison_source_month_skill_tag_hhi h
    ON r.source = h.source
    AND r.posted_month = h.posted_month
    AND r.run_id = h.run_id
    AND h.month_in_strict_intersection = TRUE;

-- -----------------------------------------------------------------------------
-- Legacy cleanup (optional): older phase used comparison_region_* and region_label.
-- Run once if those views still exist in your catalog:
--   DROP VIEW IF EXISTS jmi_analytics_v2.comparison_region_skill_mix_aligned_top20;
--   DROP VIEW IF EXISTS jmi_analytics_v2.comparison_region_skill_mix;
--   DROP VIEW IF EXISTS jmi_analytics_v2.comparison_region_month_totals;
-- -----------------------------------------------------------------------------