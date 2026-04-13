-- Adzuna radar profile — jmi_analytics_v2 only.
-- Long format: one row per (posted_month, axis). Values are rates in [0, 1] (comparable on a radar).
-- Depends on: v2_in_silver_jobs_base, v2_in_geo_location_rules

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_radar_profile_monthly AS
WITH base AS (
  SELECT *
  FROM jmi_analytics_v2.v2_in_silver_jobs_base
  WHERE source = 'adzuna_in'
),
monthly_rates AS (
  SELECT
    b.source,
    b.posted_month,
    max(b.bronze_run_id) AS latest_bronze_run_id,
    CAST(COUNT(*) AS double) AS n_jobs,
    CAST(SUM(CASE WHEN lower(trim(b.remote_type)) <> 'unknown' THEN 1 ELSE 0 END) AS double)
      / CAST(COUNT(*) AS double) AS remote_classified_share,
    CAST(SUM(
      CASE
        WHEN COALESCE(cardinality(TRY_CAST(json_parse(b.skills_json) AS array(varchar))), 0) > 0 THEN 1
        ELSE 0
      END
    ) AS double) / CAST(COUNT(*) AS double) AS skill_tagged_share,
    CAST(SUM(
      CASE
        WHEN trim(b.location_raw) <> '' AND lower(trim(b.location_raw)) <> 'india' THEN 1
        ELSE 0
      END
    ) AS double) / CAST(COUNT(*) AS double) AS location_subnational_share
  FROM base b
  GROUP BY b.source, b.posted_month
),
geo_join AS (
  SELECT
    b.posted_month,
    g.india_state_name,
    COUNT(*) AS c
  FROM base b
  INNER JOIN jmi_analytics_v2.v2_in_geo_location_rules g ON b.job_id = g.job_id
  GROUP BY b.posted_month, g.india_state_name
),
geo_ranked AS (
  SELECT
    posted_month,
    india_state_name,
    c,
    ROW_NUMBER() OVER (PARTITION BY posted_month ORDER BY c DESC) AS rn
  FROM geo_join
),
geo_top3 AS (
  SELECT
    posted_month,
    CAST(SUM(c) AS double) AS top3_c
  FROM geo_ranked
  WHERE rn <= 3
  GROUP BY posted_month
),
geo_totals AS (
  SELECT posted_month, CAST(SUM(c) AS double) AS total_c
  FROM geo_join
  GROUP BY posted_month
),
state_top3_share AS (
  SELECT
    g.posted_month,
    g.top3_c / t.total_c AS geography_top3_state_share
  FROM geo_top3 g
  INNER JOIN geo_totals t ON g.posted_month = t.posted_month
),
comp_counts AS (
  SELECT
    posted_month,
    lower(trim(company_norm)) AS company_norm,
    COUNT(*) AS c
  FROM base
  WHERE trim(company_norm) <> ''
  GROUP BY posted_month, lower(trim(company_norm))
),
comp_ranked AS (
  SELECT
    posted_month,
    company_norm,
    c,
    ROW_NUMBER() OVER (PARTITION BY posted_month ORDER BY c DESC) AS rn
  FROM comp_counts
),
comp_top5 AS (
  SELECT
    posted_month,
    CAST(SUM(c) AS double) AS top5_c
  FROM comp_ranked
  WHERE rn <= 5
  GROUP BY posted_month
),
employer_top5_share AS (
  SELECT
    c5.posted_month,
    c5.top5_c / m.n_jobs AS employer_top5_concentration_share
  FROM comp_top5 c5
  INNER JOIN (
    SELECT posted_month, CAST(COUNT(*) AS double) AS n_jobs FROM base GROUP BY posted_month
  ) m ON c5.posted_month = m.posted_month
),
axes AS (
  SELECT
    m.source,
    m.posted_month,
    m.latest_bronze_run_id,
    CAST('adzuna_market' AS varchar) AS profile_name,
    CAST('remote_classified_share' AS varchar) AS axis_name,
    m.remote_classified_share AS axis_value,
    CAST('Share of jobs with remote_type not unknown' AS varchar) AS axis_description
  FROM monthly_rates m
  UNION ALL
  SELECT
    m.source,
    m.posted_month,
    m.latest_bronze_run_id,
    'adzuna_market',
    'skill_tagged_share',
    m.skill_tagged_share,
    'Share of jobs with at least one extracted skill token'
  FROM monthly_rates m
  UNION ALL
  SELECT
    m.source,
    m.posted_month,
    m.latest_bronze_run_id,
    'adzuna_market',
    'location_subnational_share',
    m.location_subnational_share,
    'Share of jobs with location more specific than country-only'
  FROM monthly_rates m
  UNION ALL
  SELECT
    m.source,
    m.posted_month,
    m.latest_bronze_run_id,
    'adzuna_market',
    'geography_top3_state_share',
    s.geography_top3_state_share,
    'Share of jobs in the three largest state buckets (from geo rules, incl. unmapped buckets)'
  FROM monthly_rates m
  INNER JOIN state_top3_share s ON m.posted_month = s.posted_month
  UNION ALL
  SELECT
    m.source,
    m.posted_month,
    m.latest_bronze_run_id,
    'adzuna_market',
    'employer_top5_concentration_share',
    e.employer_top5_concentration_share,
    'Share of all jobs accounted for by the top 5 employers by count'
  FROM monthly_rates m
  INNER JOIN employer_top5_share e ON m.posted_month = e.posted_month
)
SELECT
  profile_name,
  axis_name,
  axis_value,
  axis_description,
  source,
  posted_month,
  latest_bronze_run_id AS bronze_run_id
FROM axes;
