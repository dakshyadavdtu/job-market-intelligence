-- comparison_v2_views.sql — Replace thin jmi_gold_v2.derived_* wrappers with view-first v2_* in jmi_analytics_v2.
-- Prerequisites: deploy_athena_comparison_views_v2.py (comparison_* views) and jmi_gold_v2 facts.
-- v2_strict_common_* run_id_* columns use the Gold run_id for strict_intersection_latest_month (per-month MAX(run_id)), not the global latest_run_metadata_arbeitnow pointer.
-- Run order: CREATE OR REPLACE VIEW statements first, then DROP TABLE for obsolete external tables.

-- -----------------------------------------------------------------------------
-- v2_strict_common_* (from comparison_*; run_id_* from strict intersection latest month)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_strict_common_month_totals AS
SELECT
  source,
  posted_month,
  run_id,
  total_postings,
  CAST('strict_common_month' AS VARCHAR) AS layer_scope
FROM jmi_analytics_v2.comparison_strict_intersection_month_totals;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_strict_common_benchmark_summary AS
WITH pol AS (SELECT * FROM jmi_analytics_v2.comparison_time_window_policy LIMIT 1)
SELECT
  CAST('strict_common_month' AS VARCHAR) AS layer_scope,
  p.strict_intersection_latest_month,
  p.strict_intersection_month_count,
  p.march_strict_comparable_both_sources,
  (SELECT run_id FROM jmi_analytics_v2.comparison_strict_intersection_month_totals t
   WHERE t.source = 'arbeitnow'
     AND t.posted_month = (SELECT MAX(m.posted_month) FROM jmi_analytics_v2.comparison_strict_intersection_months m)
   LIMIT 1) AS run_id_arbeitnow,
  (SELECT run_id FROM jmi_analytics_v2.comparison_strict_intersection_month_totals t
   WHERE t.source = 'adzuna_in'
     AND t.posted_month = (SELECT MAX(m.posted_month) FROM jmi_analytics_v2.comparison_strict_intersection_months m)
   LIMIT 1) AS run_id_adzuna_in,
  date_format(at_timezone(current_timestamp, 'UTC'), '%Y-%m-%dT%H:%i:%SZ') AS materialized_at_utc
FROM pol p;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_strict_common_manifest AS
WITH inter AS (SELECT posted_month FROM jmi_analytics_v2.comparison_strict_intersection_months),
     agg AS (
       SELECT
         COALESCE(array_join(array_agg(posted_month ORDER BY posted_month), ','), '') AS strict_months_csv,
         CAST(COUNT(*) AS BIGINT) AS strict_intersection_month_count,
         MAX(posted_month) AS strict_intersection_latest_month,
         CAST(COALESCE(MAX(CASE WHEN posted_month LIKE '%-03' THEN 1 ELSE 0 END), 0) >= 1 AS BOOLEAN) AS march_in_strict_intersection
       FROM inter
     )
SELECT
  CAST('strict_common_month' AS VARCHAR) AS layer_scope,
  (SELECT run_id FROM jmi_analytics_v2.comparison_strict_intersection_month_totals t
   WHERE t.source = 'arbeitnow'
     AND t.posted_month = (SELECT MAX(m.posted_month) FROM jmi_analytics_v2.comparison_strict_intersection_months m)
   LIMIT 1) AS run_id_arbeitnow,
  (SELECT run_id FROM jmi_analytics_v2.comparison_strict_intersection_month_totals t
   WHERE t.source = 'adzuna_in'
     AND t.posted_month = (SELECT MAX(m.posted_month) FROM jmi_analytics_v2.comparison_strict_intersection_months m)
   LIMIT 1) AS run_id_adzuna_in,
  a.strict_months_csv,
  a.strict_intersection_month_count,
  a.strict_intersection_latest_month,
  a.march_in_strict_intersection,
  date_format(at_timezone(current_timestamp, 'UTC'), '%Y-%m-%dT%H:%i:%SZ') AS materialized_at_utc
FROM agg a;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_strict_common_skill_mix AS
SELECT
  skill,
  job_count,
  CAST(NULL AS VARCHAR) AS bronze_ingest_date,
  CAST(NULL AS VARCHAR) AS bronze_run_id,
  CAST(NULL AS VARCHAR) AS time_axis,
  source,
  posted_month,
  run_id,
  CAST('strict_common_month' AS VARCHAR) AS layer_scope
FROM jmi_analytics_v2.comparison_strict_intersection_skill_demand;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_strict_common_role_mix AS
SELECT
  role_title AS role,
  job_count,
  CAST(NULL AS VARCHAR) AS bronze_ingest_date,
  CAST(NULL AS VARCHAR) AS bronze_run_id,
  CAST(NULL AS VARCHAR) AS time_axis,
  source,
  posted_month,
  run_id,
  CAST('strict_common_month' AS VARCHAR) AS layer_scope
FROM jmi_analytics_v2.comparison_strict_intersection_role_demand;

-- -----------------------------------------------------------------------------
-- v2_march_strict_* (March rows within strict intersection)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_march_strict_month_totals AS
SELECT
  source,
  posted_month,
  run_id,
  total_postings,
  CAST('march_strict_intersection' AS VARCHAR) AS layer_scope
FROM jmi_analytics_v2.comparison_strict_intersection_month_totals
WHERE posted_month LIKE '%-03';

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_march_strict_benchmark_summary AS
WITH lr_an AS (SELECT run_id FROM jmi_gold_v2.latest_run_metadata_arbeitnow LIMIT 1),
     lr_ad AS (SELECT run_id FROM jmi_gold_v2.latest_run_metadata_adzuna LIMIT 1),
     inter AS (SELECT posted_month FROM jmi_analytics_v2.comparison_strict_intersection_months WHERE posted_month LIKE '%-03'),
     m AS (
       SELECT
         MAX(posted_month) AS march_strict_latest_month,
         CAST(COUNT(*) AS BIGINT) AS march_month_count
       FROM inter
     )
SELECT
  CAST('march_strict_intersection' AS VARCHAR) AS layer_scope,
  m.march_strict_latest_month,
  m.march_month_count,
  lr_an.run_id AS run_id_arbeitnow,
  lr_ad.run_id AS run_id_adzuna_in,
  date_format(at_timezone(current_timestamp, 'UTC'), '%Y-%m-%dT%H:%i:%SZ') AS materialized_at_utc
FROM m
CROSS JOIN lr_an
CROSS JOIN lr_ad;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_march_strict_manifest AS
WITH lr_an AS (SELECT run_id FROM jmi_gold_v2.latest_run_metadata_arbeitnow LIMIT 1),
     lr_ad AS (SELECT run_id FROM jmi_gold_v2.latest_run_metadata_adzuna LIMIT 1),
     inter AS (SELECT posted_month FROM jmi_analytics_v2.comparison_strict_intersection_months WHERE posted_month LIKE '%-03'),
     strict_all AS (SELECT posted_month FROM jmi_analytics_v2.comparison_strict_intersection_months),
     march_agg AS (
       SELECT
         COALESCE(array_join(array_agg(posted_month ORDER BY posted_month), ','), '') AS march_csv,
         CAST(COUNT(*) AS BIGINT) AS march_month_count
       FROM inter
     ),
     strict_agg AS (
       SELECT COALESCE(array_join(array_agg(posted_month ORDER BY posted_month), ','), '') AS strict_csv
       FROM strict_all
     )
SELECT
  CAST('march_strict_intersection' AS VARCHAR) AS layer_scope,
  lr_an.run_id AS run_id_arbeitnow,
  lr_ad.run_id AS run_id_adzuna_in,
  m.march_csv AS march_posted_months_csv,
  m.march_month_count,
  s.strict_csv AS strict_intersection_superset_csv,
  date_format(at_timezone(current_timestamp, 'UTC'), '%Y-%m-%dT%H:%i:%SZ') AS materialized_at_utc
FROM march_agg m
CROSS JOIN strict_agg s
CROSS JOIN lr_an
CROSS JOIN lr_ad;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_march_strict_skill_mix AS
SELECT
  skill,
  job_count,
  CAST(NULL AS VARCHAR) AS bronze_ingest_date,
  CAST(NULL AS VARCHAR) AS bronze_run_id,
  CAST(NULL AS VARCHAR) AS time_axis,
  source,
  posted_month,
  run_id,
  CAST('march_strict_intersection' AS VARCHAR) AS layer_scope
FROM jmi_analytics_v2.comparison_strict_intersection_skill_demand
WHERE posted_month LIKE '%-03';

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_march_strict_role_mix AS
SELECT
  role_title AS role,
  job_count,
  CAST(NULL AS VARCHAR) AS bronze_ingest_date,
  CAST(NULL AS VARCHAR) AS bronze_run_id,
  CAST(NULL AS VARCHAR) AS time_axis,
  source,
  posted_month,
  run_id,
  CAST('march_strict_intersection' AS VARCHAR) AS layer_scope
FROM jmi_analytics_v2.comparison_strict_intersection_role_demand
WHERE posted_month LIKE '%-03';

-- -----------------------------------------------------------------------------
-- v2_yearly_exploratory_* (exploratory calendar-year rollup from comparison_*)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_yearly_exploratory_source_year_totals AS
WITH lr_an AS (SELECT run_id FROM jmi_gold_v2.latest_run_metadata_arbeitnow LIMIT 1),
     lr_ad AS (SELECT run_id FROM jmi_gold_v2.latest_run_metadata_adzuna LIMIT 1)
SELECT
  y.source,
  CAST(y.calendar_year AS BIGINT) AS calendar_year,
  y.total_postings,
  y.months_present_in_year,
  CASE WHEN y.source = 'arbeitnow' THEN lr_an.run_id ELSE lr_ad.run_id END AS run_id,
  CAST('exploratory_latest_run_calendar_year' AS VARCHAR) AS layer_scope,
  CAST('per_source_latest_gold_run_not_strict_month_intersection' AS VARCHAR) AS data_alignment,
  CAST(TRUE AS BOOLEAN) AS exploratory_only,
  date_format(at_timezone(current_timestamp, 'UTC'), '%Y-%m-%dT%H:%i:%SZ') AS materialized_at_utc
FROM jmi_analytics_v2.comparison_exploratory_calendar_year_totals y
CROSS JOIN lr_an
CROSS JOIN lr_ad;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_yearly_exploratory_manifest AS
WITH years AS (
  SELECT DISTINCT calendar_year
  FROM jmi_analytics_v2.comparison_exploratory_calendar_year_totals
),
agg AS (
  SELECT
    COALESCE(array_join(array_agg(calendar_year ORDER BY calendar_year), ','), '') AS distinct_calendar_years_union_csv,
    CAST(COUNT(*) AS BIGINT) AS distinct_year_count_union,
    CAST(COUNT(*) AS BIGINT) >= 2 AS multi_calendar_year_data_present
  FROM years
)
SELECT
  CAST('exploratory_latest_run_calendar_year' AS VARCHAR) AS layer_scope,
  CAST(TRUE AS BOOLEAN) AS exploratory_only,
  a.distinct_calendar_years_union_csv,
  a.distinct_year_count_union,
  a.multi_calendar_year_data_present,
  a.multi_calendar_year_data_present AS headline_multi_year_narrative_worthy,
  CAST('Rollup uses each source''s latest Gold run only; months are not filtered to strict cross-source intersection. Do not treat as apples-to-apples annual benchmark across sources unless policy views agree.' AS VARCHAR) AS note,
  date_format(at_timezone(current_timestamp, 'UTC'), '%Y-%m-%dT%H:%i:%SZ') AS materialized_at_utc
FROM agg a;

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
