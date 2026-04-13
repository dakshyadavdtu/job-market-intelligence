-- Compact Adzuna KPI slice — jmi_analytics_v2 only.
-- Replaces missing sheet1_kpis_adzuna in v2 catalog: Gold v2 totals + concentration metrics,
-- plus remote_classified_share from v2_in_silver_jobs_base (not in Gold).
-- Latest pipeline run only (jmi_gold_v2.latest_run_metadata_adzuna), mirroring sheet1 intent.
-- Does NOT depend on role_group_demand_monthly_adzuna.

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_kpi_slice_monthly AS
WITH lr AS (
  SELECT run_id FROM jmi_gold_v2.latest_run_metadata_adzuna LIMIT 1
),
run_months AS (
  SELECT
    r.run_id,
    CAST(COUNT(DISTINCT r.posted_month) AS bigint) AS active_posted_months
  FROM jmi_gold_v2.role_demand_monthly r
  INNER JOIN lr ON r.run_id = lr.run_id
  WHERE r.source = 'adzuna_in'
    AND r.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY r.run_id
),
role_totals AS (
  SELECT
    r.posted_month,
    r.run_id,
    SUM(r.job_count) AS total_jobs,
    MAX(r.job_count) AS max_role_job_count
  FROM jmi_gold_v2.role_demand_monthly r
  INNER JOIN lr ON r.run_id = lr.run_id
  WHERE r.source = 'adzuna_in'
    AND r.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY r.posted_month, r.run_id
),
loc_totals AS (
  SELECT
    l.posted_month,
    l.run_id,
    SUM(l.job_count) AS located_jobs
  FROM jmi_gold_v2.location_demand_monthly l
  INNER JOIN lr ON l.run_id = lr.run_id
  WHERE l.source = 'adzuna_in'
    AND l.posted_month BETWEEN '2018-01' AND '2035-12'
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
    FROM jmi_gold_v2.location_demand_monthly l
    INNER JOIN lr ON l.run_id = lr.run_id
    WHERE l.source = 'adzuna_in'
      AND l.posted_month BETWEEN '2018-01' AND '2035-12'
  ) x
  WHERE rn <= 3
  GROUP BY posted_month, run_id
),
loc_max AS (
  SELECT
    l.posted_month,
    l.run_id,
    MAX(l.job_count) AS max_location_job_count
  FROM jmi_gold_v2.location_demand_monthly l
  INNER JOIN lr ON l.run_id = lr.run_id
  WHERE l.source = 'adzuna_in'
    AND l.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY l.posted_month, l.run_id
),
loc_hhi_calc AS (
  SELECT
    l.posted_month,
    l.run_id,
    SUM(
      POWER(
        CAST(l.job_count AS double) / CAST(lt.located_jobs AS double),
        2
      )
    ) AS location_hhi
  FROM jmi_gold_v2.location_demand_monthly l
  INNER JOIN lr ON l.run_id = lr.run_id
  INNER JOIN loc_totals lt
    ON l.posted_month = lt.posted_month
    AND l.run_id = lt.run_id
  WHERE lt.located_jobs > 0
    AND l.source = 'adzuna_in'
    AND l.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY l.posted_month, l.run_id
),
comp_totals AS (
  SELECT
    c.posted_month,
    c.run_id,
    SUM(c.job_count) AS company_postings_sum
  FROM jmi_gold_v2.company_hiring_monthly c
  INNER JOIN lr ON c.run_id = lr.run_id
  WHERE c.source = 'adzuna_in'
    AND c.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY c.posted_month, c.run_id
),
comp_hhi_calc AS (
  SELECT
    c.posted_month,
    c.run_id,
    SUM(
      POWER(
        CAST(c.job_count AS double) / CAST(ct.company_postings_sum AS double),
        2
      )
    ) AS company_hhi
  FROM jmi_gold_v2.company_hiring_monthly c
  INNER JOIN lr ON c.run_id = lr.run_id
  INNER JOIN comp_totals ct
    ON c.posted_month = ct.posted_month
    AND c.run_id = ct.run_id
  WHERE ct.company_postings_sum > 0
    AND c.source = 'adzuna_in'
    AND c.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY c.posted_month, c.run_id
),
loc_buckets AS (
  SELECT
    l.posted_month,
    l.run_id,
    CAST(COUNT(*) AS bigint) AS distinct_location_buckets
  FROM jmi_gold_v2.location_demand_monthly l
  INNER JOIN lr ON l.run_id = lr.run_id
  WHERE l.source = 'adzuna_in'
    AND l.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY l.posted_month, l.run_id
),
role_title_buckets AS (
  SELECT
    r.posted_month,
    r.run_id,
    CAST(COUNT(*) AS bigint) AS distinct_role_title_buckets
  FROM jmi_gold_v2.role_demand_monthly r
  INNER JOIN lr ON r.run_id = lr.run_id
  WHERE r.source = 'adzuna_in'
    AND r.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY r.posted_month, r.run_id
),
skill_tag_totals AS (
  SELECT
    s.posted_month,
    s.run_id,
    SUM(s.job_count) AS tag_sum_total
  FROM jmi_gold_v2.skill_demand_monthly s
  INNER JOIN lr ON s.run_id = lr.run_id
  WHERE s.source = 'adzuna_in'
    AND s.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY s.posted_month, s.run_id
),
skill_distinct AS (
  SELECT
    s.posted_month,
    s.run_id,
    CAST(COUNT(DISTINCT s.skill) AS bigint) AS distinct_skill_tags
  FROM jmi_gold_v2.skill_demand_monthly s
  INNER JOIN lr ON s.run_id = lr.run_id
  WHERE s.source = 'adzuna_in'
    AND s.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY s.posted_month, s.run_id
),
skill_hhi_calc AS (
  SELECT
    s.posted_month,
    s.run_id,
    SUM(
      POWER(
        CAST(s.job_count AS double) / CAST(NULLIF(t.tag_sum_total, 0) AS double),
        2
      )
    ) AS skill_tag_hhi
  FROM jmi_gold_v2.skill_demand_monthly s
  INNER JOIN lr ON s.run_id = lr.run_id
  INNER JOIN skill_tag_totals t
    ON s.posted_month = t.posted_month
    AND s.run_id = t.run_id
  WHERE s.source = 'adzuna_in'
    AND s.posted_month BETWEEN '2018-01' AND '2035-12'
  GROUP BY s.posted_month, s.run_id
),
silver_remote AS (
  SELECT
    posted_month,
    CAST(
      SUM(CASE WHEN lower(trim(remote_type)) <> 'unknown' THEN 1 ELSE 0 END) AS double
    ) / CAST(NULLIF(COUNT(*), 0) AS double) AS remote_classified_share
  FROM jmi_analytics_v2.v2_in_silver_jobs_base
  WHERE source = 'adzuna_in'
  GROUP BY posted_month
)
SELECT
  CAST('adzuna_in' AS varchar) AS source,
  r.posted_month,
  r.run_id,
  rm.active_posted_months,
  r.total_jobs,
  COALESCE(l.located_jobs, CAST(0 AS bigint)) AS located_jobs,
  CASE
    WHEN COALESCE(l.located_jobs, 0) > 0
      THEN CAST(COALESCE(t3.top3_location_job_sum, 0) AS double) / CAST(l.located_jobs AS double)
    ELSE NULL
  END AS top3_location_share,
  CASE
    WHEN COALESCE(l.located_jobs, 0) > 0 AND lm.max_location_job_count IS NOT NULL
      THEN CAST(lm.max_location_job_count AS double) / CAST(l.located_jobs AS double)
    ELSE NULL
  END AS top1_location_share,
  lh.location_hhi,
  ch.company_hhi,
  CASE
    WHEN r.total_jobs > 0
      THEN CAST(r.max_role_job_count AS double) / CAST(r.total_jobs AS double)
    ELSE NULL
  END AS top1_role_share,
  sh.skill_tag_hhi,
  sd.distinct_skill_tags,
  lb.distinct_location_buckets,
  rtb.distinct_role_title_buckets,
  sr.remote_classified_share
FROM role_totals r
INNER JOIN run_months rm ON r.run_id = rm.run_id
LEFT JOIN loc_totals l ON r.posted_month = l.posted_month AND r.run_id = l.run_id
LEFT JOIN loc_top3 t3 ON r.posted_month = t3.posted_month AND r.run_id = t3.run_id
LEFT JOIN loc_max lm ON r.posted_month = lm.posted_month AND r.run_id = lm.run_id
LEFT JOIN loc_hhi_calc lh ON r.posted_month = lh.posted_month AND r.run_id = lh.run_id
LEFT JOIN comp_hhi_calc ch ON r.posted_month = ch.posted_month AND r.run_id = ch.run_id
LEFT JOIN loc_buckets lb ON r.posted_month = lb.posted_month AND r.run_id = lb.run_id
LEFT JOIN role_title_buckets rtb ON r.posted_month = rtb.posted_month AND r.run_id = rtb.run_id
LEFT JOIN skill_distinct sd ON r.posted_month = sd.posted_month AND r.run_id = sd.run_id
LEFT JOIN skill_hhi_calc sh ON r.posted_month = sh.posted_month AND r.run_id = sh.run_id
LEFT JOIN silver_remote sr ON r.posted_month = sr.posted_month;
