-- Europe (Arbeitnow) KPI slice — jmi_analytics_v2 only.
-- Gold facts: merged GROUP BYs + window-based HHI (fewer table scans than many parallel CTEs).
-- remote_classified_share is NULL here; use v2_eu_silver_remote_classified_monthly (SPICE or ad hoc).
-- Latest EU run: jmi_gold_v2.latest_run_metadata.

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_kpi_slice_monthly AS
WITH lr AS (
  SELECT run_id FROM jmi_gold_v2.latest_run_metadata LIMIT 1
),
role_f AS (
  SELECT r.posted_month, r.run_id, r."role", r.job_count
  FROM jmi_gold_v2.role_demand_monthly r
  INNER JOIN lr ON r.run_id = lr.run_id
  WHERE r.source = 'arbeitnow'
    AND r.posted_month BETWEEN '2018-01' AND '2035-12'
),
loc_f AS (
  SELECT l.posted_month, l.run_id, l.location, l.job_count
  FROM jmi_gold_v2.location_demand_monthly l
  INNER JOIN lr ON l.run_id = lr.run_id
  WHERE l.source = 'arbeitnow'
    AND l.posted_month BETWEEN '2018-01' AND '2035-12'
),
skill_f AS (
  SELECT s.posted_month, s.run_id, s.skill, s.job_count
  FROM jmi_gold_v2.skill_demand_monthly s
  INNER JOIN lr ON s.run_id = lr.run_id
  WHERE s.source = 'arbeitnow'
    AND s.posted_month BETWEEN '2018-01' AND '2035-12'
),
comp_f AS (
  SELECT c.posted_month, c.run_id, c.company_name, c.job_count
  FROM jmi_gold_v2.company_hiring_monthly c
  INNER JOIN lr ON c.run_id = lr.run_id
  WHERE c.source = 'arbeitnow'
    AND c.posted_month BETWEEN '2018-01' AND '2035-12'
),
run_months AS (
  SELECT
    r.run_id,
    CAST(COUNT(DISTINCT r.posted_month) AS bigint) AS active_posted_months
  FROM role_f r
  GROUP BY r.run_id
),
role_totals AS (
  SELECT
    r.posted_month,
    r.run_id,
    SUM(r.job_count) AS total_jobs,
    MAX(r.job_count) AS max_role_job_count,
    CAST(COUNT(*) AS bigint) AS distinct_role_title_buckets
  FROM role_f r
  GROUP BY r.posted_month, r.run_id
),
loc_core AS (
  SELECT
    l.posted_month,
    l.run_id,
    SUM(l.job_count) AS located_jobs,
    MAX(l.job_count) AS max_location_job_count,
    CAST(COUNT(*) AS bigint) AS distinct_location_buckets
  FROM loc_f l
  GROUP BY l.posted_month, l.run_id
),
loc_top3 AS (
  SELECT
    posted_month,
    run_id,
    SUM(job_count) AS top3_location_job_sum
  FROM (
    SELECT
      l.posted_month,
      l.run_id,
      l.job_count,
      ROW_NUMBER() OVER (
        PARTITION BY l.posted_month, l.run_id
        ORDER BY l.job_count DESC, l.location ASC
      ) AS rn
    FROM loc_f l
  ) x
  WHERE rn <= 3
  GROUP BY posted_month, run_id
),
loc_hhi_calc AS (
  SELECT
    posted_month,
    run_id,
    SUM(
      POWER(
        CAST(x.job_count AS double) / CAST(NULLIF(tot, 0) AS double),
        2
      )
    ) AS location_hhi
  FROM (
    SELECT
      l.posted_month,
      l.run_id,
      l.job_count,
      SUM(l.job_count) OVER (PARTITION BY l.posted_month, l.run_id) AS tot
    FROM loc_f l
  ) x
  GROUP BY posted_month, run_id
),
comp_agg AS (
  SELECT
    posted_month,
    run_id,
    MAX(tot) AS company_postings_sum,
    SUM(
      POWER(
        CAST(x.job_count AS double) / CAST(NULLIF(tot, 0) AS double),
        2
      )
    ) AS company_hhi
  FROM (
    SELECT
      c.posted_month,
      c.run_id,
      c.company_name,
      c.job_count,
      SUM(c.job_count) OVER (PARTITION BY c.posted_month, c.run_id) AS tot
    FROM comp_f c
  ) x
  GROUP BY posted_month, run_id
),
skill_core AS (
  SELECT
    s.posted_month,
    s.run_id,
    SUM(s.job_count) AS tag_sum_total,
    CAST(COUNT(DISTINCT s.skill) AS bigint) AS distinct_skill_tags
  FROM skill_f s
  GROUP BY s.posted_month, s.run_id
),
skill_hhi_calc AS (
  SELECT
    posted_month,
    run_id,
    SUM(
      POWER(
        CAST(x.job_count AS double) / CAST(NULLIF(tot, 0) AS double),
        2
      )
    ) AS skill_tag_hhi
  FROM (
    SELECT
      s.posted_month,
      s.run_id,
      s.skill,
      s.job_count,
      SUM(s.job_count) OVER (PARTITION BY s.posted_month, s.run_id) AS tot
    FROM skill_f s
  ) x
  GROUP BY posted_month, run_id
),
skill_top5 AS (
  SELECT
    posted_month,
    run_id,
    SUM(job_count) AS top5_skill_tag_jobs
  FROM (
    SELECT
      s.posted_month,
      s.run_id,
      s.job_count,
      ROW_NUMBER() OVER (
        PARTITION BY s.posted_month, s.run_id
        ORDER BY s.job_count DESC, s.skill ASC
      ) AS rn
    FROM skill_f s
  ) x
  WHERE rn <= 5
  GROUP BY posted_month, run_id
)
SELECT
  CAST('arbeitnow' AS varchar) AS source,
  r.posted_month,
  r.run_id,
  rm.active_posted_months,
  r.total_jobs,
  COALESCE(lc.located_jobs, CAST(0 AS bigint)) AS located_jobs,
  CASE
    WHEN COALESCE(lc.located_jobs, 0) > 0
      THEN CAST(COALESCE(t3.top3_location_job_sum, 0) AS double) / CAST(lc.located_jobs AS double)
    ELSE NULL
  END AS top3_location_share,
  CASE
    WHEN COALESCE(lc.located_jobs, 0) > 0 AND lc.max_location_job_count IS NOT NULL
      THEN CAST(lc.max_location_job_count AS double) / CAST(lc.located_jobs AS double)
    ELSE NULL
  END AS top1_location_share,
  lh.location_hhi,
  ca.company_hhi,
  CASE
    WHEN r.total_jobs > 0
      THEN CAST(r.max_role_job_count AS double) / CAST(r.total_jobs AS double)
    ELSE NULL
  END AS top1_role_share,
  sh.skill_tag_hhi,
  sc.distinct_skill_tags,
  lc.distinct_location_buckets,
  r.distinct_role_title_buckets,
  CASE
    WHEN sc.tag_sum_total > 0 AND k5.top5_skill_tag_jobs IS NOT NULL
      THEN CAST(k5.top5_skill_tag_jobs AS double) / CAST(sc.tag_sum_total AS double)
    ELSE NULL
  END AS top5_skill_tag_share,
  CAST(NULL AS double) AS remote_classified_share
FROM role_totals r
INNER JOIN run_months rm ON r.run_id = rm.run_id
LEFT JOIN loc_core lc ON r.posted_month = lc.posted_month AND r.run_id = lc.run_id
LEFT JOIN loc_top3 t3 ON r.posted_month = t3.posted_month AND r.run_id = t3.run_id
LEFT JOIN loc_hhi_calc lh ON r.posted_month = lh.posted_month AND r.run_id = lh.run_id
LEFT JOIN comp_agg ca ON r.posted_month = ca.posted_month AND r.run_id = ca.run_id
LEFT JOIN skill_core sc ON r.posted_month = sc.posted_month AND r.run_id = sc.run_id
LEFT JOIN skill_hhi_calc sh ON r.posted_month = sh.posted_month AND r.run_id = sh.run_id
LEFT JOIN skill_top5 k5 ON r.posted_month = k5.posted_month AND r.run_id = k5.run_id;
