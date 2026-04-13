-- Adzuna cross-dimension heatmap — jmi_analytics_v2 only.
-- dim_x = India state (from geo rules, excludes unmapped_* buckets)
-- dim_y = skill token bucket: top 15 skills by mention count per posted_month + Other
-- job_count = distinct job_id per cell (jobs that have at least one skill in that bucket for that state)
--
-- Depends on: v2_in_silver_jobs_skills_long, v2_in_geo_location_rules

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_heatmap_state_skill_monthly AS
WITH long_sk AS (
  SELECT
    s.job_id,
    s.source,
    s.posted_month,
    s.bronze_run_id,
    lower(trim(s.skill_token)) AS skill_token,
    g.india_state_name AS state_name
  FROM jmi_analytics_v2.v2_in_silver_jobs_skills_long s
  INNER JOIN jmi_analytics_v2.v2_in_geo_location_rules g
    ON s.job_id = g.job_id
  WHERE s.source = 'adzuna_in'
    AND g.india_state_name NOT LIKE 'unmapped%'
),
skill_rank AS (
  SELECT
    posted_month,
    skill_token,
    COUNT(*) AS mention_count,
    ROW_NUMBER() OVER (
      PARTITION BY posted_month
      ORDER BY COUNT(*) DESC
    ) AS rn
  FROM long_sk
  GROUP BY posted_month, skill_token
),
bucketed AS (
  SELECT
    ls.job_id,
    ls.source,
    ls.posted_month,
    ls.bronze_run_id,
    ls.state_name,
    CASE
      WHEN sr.rn IS NOT NULL AND sr.rn <= 15 THEN ls.skill_token
      ELSE 'Other'
    END AS skill_bucket
  FROM long_sk ls
  LEFT JOIN skill_rank sr
    ON ls.posted_month = sr.posted_month
    AND ls.skill_token = sr.skill_token
)
SELECT
  source,
  posted_month,
  max(bronze_run_id) AS latest_bronze_run_id,
  state_name AS dim_x,
  skill_bucket AS dim_y,
  CAST(COUNT(DISTINCT job_id) AS bigint) AS job_count
FROM bucketed
GROUP BY source, posted_month, state_name, skill_bucket;
