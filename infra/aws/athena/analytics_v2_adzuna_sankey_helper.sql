-- Adzuna Sankey helper — jmi_analytics_v2 only.
-- Row-level Silver + geo join + window ranks: expensive for QuickSight Direct Query — prefer SPICE.
-- Design: state (India, from geo rules) -> company bucket (top 10 employers per posted_month + Other).
-- Joins row-level geo rules to Silver base for company_norm. Excludes unmapped states and empty employers.

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_sankey_state_to_company_monthly AS
WITH job_enriched AS (
  SELECT
    g.job_id,
    b.source,
    b.posted_month,
    b.bronze_run_id,
    lower(trim(b.company_norm)) AS company_norm,
    g.india_state_name AS state_name
  FROM jmi_analytics_v2.v2_in_geo_location_rules g
  INNER JOIN jmi_analytics_v2.v2_in_silver_jobs_base b
    ON g.job_id = b.job_id
    AND b.source = 'adzuna_in'
  WHERE g.india_state_name NOT LIKE 'unmapped%'
    AND trim(b.company_norm) <> ''
),
company_rank AS (
  SELECT
    posted_month,
    company_norm,
    cnt,
    ROW_NUMBER() OVER (PARTITION BY posted_month ORDER BY cnt DESC) AS rnk
  FROM (
    SELECT
      posted_month,
      company_norm,
      COUNT(*) AS cnt
    FROM job_enriched
    GROUP BY posted_month, company_norm
  ) t
),
bucketed AS (
  SELECT
    je.job_id,
    je.source,
    je.posted_month,
    je.bronze_run_id,
    je.state_name,
    CASE
      WHEN cr.rnk IS NOT NULL AND cr.rnk <= 10 THEN je.company_norm
      ELSE 'Other'
    END AS company_bucket
  FROM job_enriched je
  LEFT JOIN company_rank cr
    ON je.posted_month = cr.posted_month
    AND je.company_norm = cr.company_norm
)
SELECT
  source,
  posted_month,
  max(bronze_run_id) AS latest_bronze_run_id,
  state_name AS source_bucket,
  company_bucket AS target_bucket,
  CAST(COUNT(*) AS bigint) AS edge_weight
FROM bucketed
GROUP BY source, posted_month, state_name, company_bucket;
