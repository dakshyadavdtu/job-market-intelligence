-- Europe (Arbeitnow) Sankey helper — jmi_analytics_v2 only.
-- Row-level Silver + window ranks: expensive for QuickSight Direct Query — prefer SPICE or pre-aggregate.
-- Flow: primary location bucket (top 10 per posted_month + Other) -> company bucket (top 10 + Other).
-- Mirrors v2_in_sankey_state_to_company_monthly: row-level Silver, controlled bucketing, no Gold role-title classifier duplication.
-- Location key: first segment before comma (trim, lower) to reduce "City, Country" fragmentation.
-- posted_month may be NULL when posted_at does not parse; those rows rank in a separate cohort. Filter in QS if a single-month Sankey is required.

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_sankey_location_to_company_monthly AS
WITH job_base AS (
  SELECT
    job_id,
    source,
    posted_month,
    bronze_run_id,
    lower(trim(company_norm)) AS company_key,
    lower(
      trim(
        CASE
          WHEN strpos(trim(location_raw), ',') > 0
            THEN substr(trim(location_raw), 1, strpos(trim(location_raw), ',') - 1)
          ELSE trim(location_raw)
        END
      )
    ) AS location_key
  FROM jmi_analytics_v2.v2_eu_silver_jobs_base
  WHERE source = 'arbeitnow'
    AND trim(COALESCE(company_norm, '')) <> ''
),
job_enriched AS (
  SELECT
    job_id,
    source,
    posted_month,
    bronze_run_id,
    company_key,
    CASE
      WHEN location_key IS NULL OR location_key = '' THEN '(unknown location)'
      ELSE location_key
    END AS location_key
  FROM job_base
),
loc_rank AS (
  SELECT
    posted_month,
    location_key,
    cnt,
    ROW_NUMBER() OVER (PARTITION BY posted_month ORDER BY cnt DESC, location_key ASC) AS rnk
  FROM (
    SELECT
      posted_month,
      location_key,
      COUNT(*) AS cnt
    FROM job_enriched
    GROUP BY posted_month, location_key
  ) t
),
company_rank AS (
  SELECT
    posted_month,
    company_key,
    cnt,
    ROW_NUMBER() OVER (PARTITION BY posted_month ORDER BY cnt DESC, company_key ASC) AS rnk
  FROM (
    SELECT
      posted_month,
      company_key,
      COUNT(*) AS cnt
    FROM job_enriched
    GROUP BY posted_month, company_key
  ) t
),
bucketed AS (
  SELECT
    je.job_id,
    je.source,
    je.posted_month,
    je.bronze_run_id,
    CASE
      WHEN lr.rnk IS NOT NULL AND lr.rnk <= 10 THEN je.location_key
      ELSE 'Other (locations)'
    END AS location_bucket,
    CASE
      WHEN cr.rnk IS NOT NULL AND cr.rnk <= 10 THEN je.company_key
      ELSE 'Other (employers)'
    END AS company_bucket
  FROM job_enriched je
  LEFT JOIN loc_rank lr
    ON je.posted_month = lr.posted_month
    AND je.location_key = lr.location_key
  LEFT JOIN company_rank cr
    ON je.posted_month = cr.posted_month
    AND je.company_key = cr.company_key
)
SELECT
  source,
  posted_month,
  max(bronze_run_id) AS latest_bronze_run_id,
  location_bucket AS source_bucket,
  company_bucket AS target_bucket,
  CAST(COUNT(*) AS bigint) AS edge_weight
FROM bucketed
GROUP BY source, posted_month, location_bucket, company_bucket;
