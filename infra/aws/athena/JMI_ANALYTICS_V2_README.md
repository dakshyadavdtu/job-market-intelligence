### `jmi_analytics_v2.v2_in_silver_jobs_base`

**Graph:** No single chart. Underlies India heatmap, Sankey, radar, funnel, geo, and skills-long.

**Made:** From `jmi_silver_v2.adzuna_jobs_merged`, derive `posted_month`, keep rows whose month appears in latest Adzuna Gold `role_demand_monthly`.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_silver_jobs_base AS
SELECT
  job_id,
  source,
  source_job_id,
  title_norm,
  company_norm,
  location_raw,
  remote_type,
  posted_at,
  posted_month,
  ingested_at,
  bronze_ingest_date,
  bronze_run_id,
  job_id_strategy,
  bronze_data_file,
  skills_json
FROM (
  SELECT
    job_id,
    source,
    source_job_id,
    title_norm,
    company_norm,
    location_raw,
    remote_type,
    posted_at,
    date_format(
      date_trunc(
        'month',
        COALESCE(
          TRY(date_parse(nullif(trim(substr(posted_at, 1, 10)), ''), '%Y-%m-%d')),
          TRY(cast(from_iso8601_timestamp(posted_at) AS date))
        )
      ),
      '%Y-%m'
    ) AS posted_month,
    ingested_at,
    bronze_ingest_date,
    bronze_run_id,
    job_id_strategy,
    bronze_data_file,
    skills AS skills_json
  FROM jmi_silver_v2.adzuna_jobs_merged
  WHERE source = 'adzuna_in'
) b
WHERE b.posted_month IN (
  SELECT DISTINCT r.posted_month
  FROM jmi_gold_v2.role_demand_monthly r
  INNER JOIN (SELECT run_id FROM jmi_gold_v2.latest_run_metadata_adzuna LIMIT 1) lr ON r.run_id = lr.run_id
  WHERE r.source = 'adzuna_in'
);

-- UNNEST expands one row per skill tag; heavy at scale — prefer v2_in_gold_skill_rows_monthly for Direct Query.;
```


### `jmi_analytics_v2.v2_in_silver_jobs_skills_long`

**Graph:** No single chart. One row per skill token for heatmap `dim_y` and `v2_cmp_skills_per_job_april_2026`.

**Made:** `UNNEST` JSON skills array on `v2_in_silver_jobs_base`.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_silver_jobs_skills_long AS
SELECT
  b.job_id,
  b.source,
  b.source_job_id,
  b.title_norm,
  b.company_norm,
  b.location_raw,
  b.remote_type,
  b.posted_at,
  b.posted_month,
  b.ingested_at,
  b.bronze_ingest_date,
  b.bronze_run_id,
  trim(t.skill_token) AS skill_token
FROM jmi_analytics_v2.v2_in_silver_jobs_base b
CROSS JOIN UNNEST(
  COALESCE(
    TRY_CAST(json_parse(b.skills_json) AS array(varchar)),
    CAST(ARRAY[] AS array(varchar))
  )
) AS t (skill_token)
WHERE trim(COALESCE(t.skill_token, '')) <> '';
```


### `jmi_analytics_v2.v2_eu_silver_jobs_base`

**Graph:** No single chart. Feeds EU Sankey and EU skills-long.

**Made:** `arbeitnow_jobs_merged`, rolling two UTC months, `posted_month` aligned to Gold; intersect `gold_months`.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_silver_jobs_base AS
WITH month_bounds AS (
  SELECT
    date_format(date_add('month', -1, date_trunc('month', current_timestamp)), '%Y-%m') AS pm_min,
    date_format(date_trunc('month', current_timestamp), '%Y-%m') AS pm_max
),
gold_months AS (
  SELECT DISTINCT r.posted_month
  FROM jmi_gold_v2.role_demand_monthly r
  CROSS JOIN month_bounds b
  WHERE r.source = 'arbeitnow'
    AND r.posted_month BETWEEN b.pm_min AND b.pm_max
)
SELECT
  job_id,
  source,
  source_job_id,
  title_norm,
  company_norm,
  location_raw,
  remote_type,
  posted_at,
  posted_month,
  ingested_at,
  bronze_ingest_date,
  bronze_run_id,
  job_id_strategy,
  bronze_data_file,
  skills_json
FROM (
  SELECT
    job_id,
    source,
    source_job_id,
    title_norm,
    company_norm,
    location_raw,
    remote_type,
    posted_at,
    CASE
      WHEN pm_from_posted IS NOT NULL
        AND regexp_like(pm_from_posted, '^[0-9]{4}-[0-9]{2}$')
      THEN pm_from_posted
      WHEN regexp_like(substr(trim(COALESCE(bronze_ingest_date, '')), 1, 7), '^[0-9]{4}-[0-9]{2}$')
      THEN substr(trim(bronze_ingest_date), 1, 7)
      ELSE ''
    END AS posted_month,
    ingested_at,
    bronze_ingest_date,
    bronze_run_id,
    job_id_strategy,
    bronze_data_file,
    skills AS skills_json
  FROM (
    SELECT
      job_id,
      source,
      source_job_id,
      title_norm,
      company_norm,
      location_raw,
      remote_type,
      posted_at,
      ingested_at,
      bronze_ingest_date,
      bronze_run_id,
      job_id_strategy,
      bronze_data_file,
      skills,
      date_format(
        date_trunc(
          'month',
          COALESCE(
            TRY(date_parse(nullif(trim(substr(posted_at, 1, 10)), ''), '%Y-%m-%d')),
            TRY(cast(from_iso8601_timestamp(posted_at) AS date))
          )
        ),
        '%Y-%m'
      ) AS pm_from_posted
    FROM jmi_silver_v2.arbeitnow_jobs_merged
    WHERE source = 'arbeitnow'
  ) raw
) b
WHERE b.posted_month IN (SELECT g.posted_month FROM gold_months g);
```


### `jmi_analytics_v2.v2_eu_silver_jobs_skills_long`

**Graph:** No single chart. Skill tokens for `v2_cmp_skills_per_job_april_2026`.

**Made:** `UNNEST` on `v2_eu_silver_jobs_base`.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_silver_jobs_skills_long AS
SELECT
  b.job_id,
  b.source,
  b.source_job_id,
  b.title_norm,
  b.company_norm,
  b.location_raw,
  b.remote_type,
  b.posted_at,
  b.posted_month,
  b.ingested_at,
  b.bronze_ingest_date,
  b.bronze_run_id,
  trim(t.skill_token) AS skill_token
FROM jmi_analytics_v2.v2_eu_silver_jobs_base b
CROSS JOIN UNNEST(
  COALESCE(
    TRY_CAST(json_parse(b.skills_json) AS array(varchar)),
    CAST(ARRAY[] AS array(varchar))
  )
) AS t (skill_token)
WHERE trim(COALESCE(t.skill_token, '')) <> '';
```


### `jmi_analytics_v2.v2_in_geo_location_rules`

**Graph:** Intermediate (not a chart). Join key for heatmap and Sankey.

**Made:** Parse `location_raw`; `CASE` maps cities/states to `india_state_name`.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_geo_location_rules AS
WITH base AS (
  SELECT
    job_id,
    source,
    posted_month,
    bronze_run_id,
    lower(trim(regexp_replace(regexp_replace(location_raw, '\s+', ' '), '(,)\s*', '$1'))) AS loc
  FROM jmi_analytics_v2.v2_in_silver_jobs_base
  WHERE source = 'adzuna_in'
),
seg AS (
  SELECT
    job_id,
    source,
    posted_month,
    bronze_run_id,
    loc,
    trim(split_part(loc, ',', 1)) AS p1,
    trim(split_part(loc, ',', 2)) AS p2
  FROM base
),
st AS (
  SELECT
    job_id,
    source,
    posted_month,
    bronze_run_id,
    loc,
    p1,
    p2,
    CASE
      WHEN loc = '' OR loc IS NULL THEN 'unmapped'
      WHEN loc = 'india' THEN 'unmapped_country'
      WHEN p2 = '' THEN
        CASE
          WHEN p1 IN (
            'andhra pradesh', 'arunachal pradesh', 'assam', 'bihar', 'chhattisgarh', 'goa', 'gujarat',
            'haryana', 'himachal pradesh', 'jharkhand', 'karnataka', 'kerala', 'madhya pradesh',
            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha', 'punjab',
            'rajasthan', 'sikkim', 'tamil nadu', 'telangana', 'tripura', 'uttar pradesh',
            'uttarakhand', 'west bengal', 'delhi', 'jammu and kashmir', 'ladakh', 'puducherry',
            'chandigarh', 'dadra and nagar haveli and daman and diu', 'lakshadweep',
            'andaman and nicobar islands'
          ) THEN p1
          WHEN p1 IN ('mumbai', 'pune', 'nagpur', 'nashik', 'thane', 'navi mumbai') THEN 'maharashtra'
          WHEN p1 IN ('bangalore', 'bengaluru', 'mysore', 'mysuru') THEN 'karnataka'
          WHEN p1 = 'hyderabad' THEN 'telangana'
          WHEN p1 IN ('chennai', 'coimbatore', 'madurai', 'tiruchirappalli') THEN 'tamil nadu'
          WHEN p1 = 'kolkata' THEN 'west bengal'
          WHEN p1 IN ('ahmedabad', 'surat', 'vadodara') THEN 'gujarat'
          WHEN p1 = 'jaipur' THEN 'rajasthan'
          WHEN p1 IN ('lucknow', 'kanpur', 'noida', 'ghaziabad') THEN 'uttar pradesh'
          WHEN p1 IN ('gurgaon', 'gurugram', 'faridabad') THEN 'haryana'
          WHEN p1 IN ('indore', 'bhopal') THEN 'madhya pradesh'
          WHEN p1 IN ('kozhikode', 'kochi', 'thiruvananthapuram') THEN 'kerala'
          WHEN p1 IN ('visakhapatnam', 'vijayawada', 'anantapur') THEN 'andhra pradesh'
          WHEN p1 = 'patna' THEN 'bihar'
          WHEN p1 = 'ranchi' THEN 'jharkhand'
          WHEN p1 = 'bhubaneswar' THEN 'odisha'
          WHEN p1 = 'guwahati' THEN 'assam'
          WHEN p1 = 'chandigarh' THEN 'chandigarh'
          ELSE 'unmapped_single'
        END
      WHEN p2 IN ('india', 'in') THEN
        CASE
          WHEN p1 IN (
            'andhra pradesh', 'arunachal pradesh', 'assam', 'bihar', 'chhattisgarh', 'goa', 'gujarat',
            'haryana', 'himachal pradesh', 'jharkhand', 'karnataka', 'kerala', 'madhya pradesh',
            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha', 'punjab',
            'rajasthan', 'sikkim', 'tamil nadu', 'telangana', 'tripura', 'uttar pradesh',
            'uttarakhand', 'west bengal', 'delhi', 'jammu and kashmir', 'ladakh', 'puducherry',
            'chandigarh'
          ) THEN p1
          WHEN p1 IN ('mumbai', 'pune', 'nagpur', 'nashik', 'thane', 'navi mumbai') THEN 'maharashtra'
          WHEN p1 IN ('bangalore', 'bengaluru', 'mysore', 'mysuru') THEN 'karnataka'
          WHEN p1 = 'hyderabad' THEN 'telangana'
          WHEN p1 IN ('chennai', 'coimbatore', 'madurai', 'tiruchirappalli') THEN 'tamil nadu'
          WHEN p1 = 'kolkata' THEN 'west bengal'
          WHEN p1 IN ('ahmedabad', 'surat', 'vadodara') THEN 'gujarat'
          WHEN p1 = 'jaipur' THEN 'rajasthan'
          WHEN p1 IN ('lucknow', 'kanpur', 'noida', 'ghaziabad') THEN 'uttar pradesh'
          WHEN p1 IN ('new delhi', 'delhi') THEN 'delhi'
          WHEN p1 IN ('gurgaon', 'gurugram', 'faridabad') THEN 'haryana'
          WHEN p1 IN ('indore', 'bhopal') THEN 'madhya pradesh'
          WHEN p1 IN ('kozhikode', 'kochi', 'thiruvananthapuram') THEN 'kerala'
          WHEN p1 IN ('visakhapatnam', 'vijayawada', 'anantapur') THEN 'andhra pradesh'
          WHEN p1 = 'patna' THEN 'bihar'
          WHEN p1 = 'ranchi' THEN 'jharkhand'
          WHEN p1 = 'bhubaneswar' THEN 'odisha'
          WHEN p1 = 'guwahati' THEN 'assam'
          WHEN p1 = 'dehradun' THEN 'uttarakhand'
          ELSE 'unmapped_state_india'
        END
      WHEN p1 = 'noida' AND p2 = 'ghaziabad' THEN 'uttar pradesh'
      WHEN p1 = 'kochi' AND strpos(lower(p2), 'ernakulam') > 0 THEN 'kerala'
      WHEN p1 = 'durgapur' AND strpos(lower(p2), 'bardhaman') > 0 THEN 'west bengal'
      WHEN p2 IN (
        'andhra pradesh', 'arunachal pradesh', 'assam', 'bihar', 'chhattisgarh', 'goa', 'gujarat',
        'haryana', 'himachal pradesh', 'jharkhand', 'karnataka', 'kerala', 'madhya pradesh',
        'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha', 'punjab',
        'rajasthan', 'sikkim', 'tamil nadu', 'telangana', 'tripura', 'uttar pradesh',
        'uttarakhand', 'west bengal', 'delhi', 'jammu and kashmir', 'ladakh', 'puducherry',
        'chandigarh'
      ) THEN p2
      WHEN p1 IN ('new delhi', 'delhi') AND p2 = 'delhi' THEN 'delhi'
      ELSE 'unmapped_pair'
    END AS india_state_name
  FROM seg
)
SELECT * FROM st;
```


### `jmi_analytics_v2.v2_in_geo_state_monthly`

**Graph:** QuickSight **filled map** (India state choropleth / point map when bound to lat-lon hierarchy in dataset editor).

**Made:** Group `v2_in_geo_location_rules` by `posted_month` and state; null `state_geo` for unmapped.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_geo_state_monthly AS
WITH agg AS (
  SELECT
    source,
    posted_month,
    max(bronze_run_id) AS latest_bronze_run_id,
    india_state_name AS state_name,
    array_join(
      transform(
        split(replace(india_state_name, '_', ' '), ' '),
        w -> concat(upper(substr(w, 1, 1)), substr(w, 2))
      ),
      ' '
    ) AS state_name_display,
    CAST(COUNT(*) AS bigint) AS job_count
  FROM jmi_analytics_v2.v2_in_geo_location_rules
  GROUP BY source, posted_month, india_state_name
)
SELECT
  source,
  posted_month,
  latest_bronze_run_id,
  CAST(state_name AS varchar) AS state_name,
  CAST(state_name_display AS varchar) AS state_name_display,
  CAST('India' AS varchar) AS country,
  CASE
    WHEN lower(trim(state_name)) LIKE 'unmapped%' THEN CAST(NULL AS varchar)
    ELSE CAST(TRIM(state_name_display) AS varchar)
  END AS state_geo,
  job_count
FROM agg;
```


### `jmi_analytics_v2.v2_eu_kpi_slice_monthly`

**Graph:** QuickSight **EU KPI strip** (KPIs, gauges: volume, location/company/skill HHI, top shares).

**Made:** `month_bounds`; `MAX(run_id)` per `posted_month`; join Gold role/location/skill/company; window HHI; `remote_classified_share` NULL.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_kpi_slice_monthly AS
WITH month_bounds AS (
  SELECT
    date_format(date_add('month', -1, date_trunc('month', current_timestamp)), '%Y-%m') AS pm_min,
    date_format(date_trunc('month', current_timestamp), '%Y-%m') AS pm_max
),
month_latest AS (
  SELECT r.posted_month, MAX(r.run_id) AS run_id
  FROM jmi_gold_v2.role_demand_monthly r
  CROSS JOIN month_bounds b
  WHERE r.source = 'arbeitnow'
    AND r.posted_month BETWEEN b.pm_min AND b.pm_max
  GROUP BY r.posted_month
),
role_f AS (
  SELECT r.posted_month, r.run_id, r."role", r.job_count
  FROM jmi_gold_v2.role_demand_monthly r
  INNER JOIN month_latest ml ON r.posted_month = ml.posted_month AND r.run_id = ml.run_id
  WHERE r.source = 'arbeitnow'
),
loc_f AS (
  SELECT l.posted_month, l.run_id, l.location, l.job_count
  FROM jmi_gold_v2.location_demand_monthly l
  INNER JOIN month_latest ml ON l.posted_month = ml.posted_month AND l.run_id = ml.run_id
  WHERE l.source = 'arbeitnow'
),
skill_f AS (
  SELECT s.posted_month, s.run_id, s.skill, s.job_count
  FROM jmi_gold_v2.skill_demand_monthly s
  INNER JOIN month_latest ml ON s.posted_month = ml.posted_month AND s.run_id = ml.run_id
  WHERE s.source = 'arbeitnow'
),
comp_f AS (
  SELECT c.posted_month, c.run_id, c.company_name, c.job_count
  FROM jmi_gold_v2.company_hiring_monthly c
  INNER JOIN month_latest ml ON c.posted_month = ml.posted_month AND c.run_id = ml.run_id
  WHERE c.source = 'arbeitnow'
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
```


### `jmi_analytics_v2.v2_in_kpi_slice_monthly`

**Graph:** QuickSight **India KPI strip** (same intent as `sheet1_kpis_adzuna`).

**Made:** `latest_run_metadata_adzuna` fixes `run_id`; same metrics as EU; `remote_classified_share` via Silver join.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_kpi_slice_monthly AS
WITH lr AS (
  SELECT run_id FROM jmi_gold_v2.latest_run_metadata_adzuna LIMIT 1
),
role_f AS (
  SELECT r.posted_month, r.run_id, r."role", r.job_count
  FROM jmi_gold_v2.role_demand_monthly r
  INNER JOIN lr ON r.run_id = lr.run_id
  WHERE r.source = 'adzuna_in'
    AND r.posted_month BETWEEN '2018-01' AND '2035-12'
),
loc_f AS (
  SELECT l.posted_month, l.run_id, l.location, l.job_count
  FROM jmi_gold_v2.location_demand_monthly l
  INNER JOIN lr ON l.run_id = lr.run_id
  WHERE l.source = 'adzuna_in'
    AND l.posted_month BETWEEN '2018-01' AND '2035-12'
),
skill_f AS (
  SELECT s.posted_month, s.run_id, s.skill, s.job_count
  FROM jmi_gold_v2.skill_demand_monthly s
  INNER JOIN lr ON s.run_id = lr.run_id
  WHERE s.source = 'adzuna_in'
    AND s.posted_month BETWEEN '2018-01' AND '2035-12'
),
comp_f AS (
  SELECT c.posted_month, c.run_id, c.company_name, c.job_count
  FROM jmi_gold_v2.company_hiring_monthly c
  INNER JOIN lr ON c.run_id = lr.run_id
  WHERE c.source = 'adzuna_in'
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
remote_core AS (
  SELECT
    m.posted_month,
    CAST(SUM(CASE WHEN lower(trim(a.remote_type)) <> 'unknown' THEN 1 ELSE 0 END) AS double)
      / CAST(NULLIF(COUNT(a.job_id), 0) AS double) AS remote_classified_share
  FROM role_totals m
  LEFT JOIN jmi_silver_v2.adzuna_jobs_merged a
    ON a.source = 'adzuna_in'
    AND regexp_like(substr(trim(a.posted_at), 1, 7), '^[0-9]{4}-[0-9]{2}$')
    AND substr(trim(a.posted_at), 1, 7) = m.posted_month
  GROUP BY m.posted_month
)
SELECT
  CAST('adzuna_in' AS varchar) AS source,
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
  rc.remote_classified_share
FROM role_totals r
INNER JOIN run_months rm ON r.run_id = rm.run_id
LEFT JOIN loc_core lc ON r.posted_month = lc.posted_month AND r.run_id = lc.run_id
LEFT JOIN loc_top3 t3 ON r.posted_month = t3.posted_month AND r.run_id = t3.run_id
LEFT JOIN loc_hhi_calc lh ON r.posted_month = lh.posted_month AND r.run_id = lh.run_id
LEFT JOIN comp_agg ca ON r.posted_month = ca.posted_month AND r.run_id = ca.run_id
LEFT JOIN skill_core sc ON r.posted_month = sc.posted_month AND r.run_id = sc.run_id
LEFT JOIN skill_hhi_calc sh ON r.posted_month = sh.posted_month AND r.run_id = sh.run_id
LEFT JOIN remote_core rc ON r.posted_month = rc.posted_month;
```


### `jmi_analytics_v2.v2_kpi_slice_monthly`

**Graph:** Same visuals as **`v2_in_kpi_slice_monthly`** (alias).

**Made:** `SELECT * FROM v2_in_kpi_slice_monthly`.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_kpi_slice_monthly AS
SELECT *
FROM jmi_analytics_v2.v2_in_kpi_slice_monthly;
```


### `jmi_analytics_v2.v2_eu_role_titles_classified`

**Graph:** QuickSight **classified role families** (bar, combo, table — feeds Pareto-style rollups downstream).

**Made:** Gold `role_demand_monthly` + `month_latest`; regex cleanup; `normalized_role_group` ladder.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_role_titles_classified AS
WITH month_bounds AS (
    SELECT
        date_format(date_add('month', -1, date_trunc('month', current_timestamp)), '%Y-%m') AS pm_min,
        date_format(date_trunc('month', current_timestamp), '%Y-%m') AS pm_max
),
month_latest AS (
    SELECT r.posted_month, MAX(r.run_id) AS run_id
    FROM jmi_gold_v2.role_demand_monthly r
    CROSS JOIN month_bounds b
    WHERE r.source = 'arbeitnow'
        AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month
),
base AS (
    SELECT
        r.posted_month,
        r.run_id,
        r."role" AS raw_role,
        r.job_count,
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        lower(trim(r."role")),
                                        '(?i)(\\(m/w/d\\)|\\(m/f/x\\)|\\(w/m/d\\)|\\(m/f/d\\)|\\(f/m/d\\)|\\(d/m/w\\)|\\(d/f/m\\)|\\(m/w/x\\))',
                                        ' '
                                    ),
                                    '(?i)(all genders|alle\\s+geschlechter|geschlecht\\s*egal)',
                                    ' '
                                ),
                                '(?i)ref\\.?\\s*nr\\.?\\s*:?\\s*[\\w./#-]+',
                                ' '
                            ),
                            '(?i)(job\\s*id\\s*[:#]?\\s*[\\w-]+|stellennr\\.?\\s*:?\\s*[\\w-]+)',
                            ' '
                        ),
                        '(?i)(\\(remote\\)|\\(hybrid\\)|\\(onsite\\)|\\(vor ort\\))',
                        ' '
                    ),
                    '[,;:|/\\\\.-]{2,}',
                    ' '
                ),
                '\\s+',
                ' '
            )
        ) AS c0
    FROM jmi_gold_v2.role_demand_monthly r
    INNER JOIN month_latest ml ON r.posted_month = ml.posted_month AND r.run_id = ml.run_id
    WHERE r.source = 'arbeitnow'
      AND r.posted_month BETWEEN '2018-01' AND '2035-12'
),
stripped AS (
    SELECT
        posted_month,
        run_id,
        raw_role,
        job_count,
        trim(regexp_replace(regexp_replace(c0, '^[, .;:|\\\\/-]+', ''), '[, .;:|\\\\/-]+$', '')) AS cleaned_role_title
    FROM base
),
classified AS (
    SELECT
        posted_month,
        run_id,
        raw_role,
        job_count,
        CASE
            WHEN cleaned_role_title = '' OR cleaned_role_title IS NULL THEN '(empty title)'
            ELSE cleaned_role_title
        END AS cleaned_role_title,
        CASE
            WHEN cleaned_role_title = '' OR cleaned_role_title IS NULL THEN 'unknown_other'
            -- 1 Cybersecurity (before generic "security" in other contexts if needed — narrow patterns)
            WHEN regexp_like(cleaned_role_title, '(?i)(cyber\\s*security|cybersecurity|informationssicherheit|information security|it[\\s-]*security|pentest|penetration|appsec|soc analyst|security engineer|security architect)') THEN 'cybersecurity'
            -- 2 Data / analytics / ML / BI
            WHEN regexp_like(cleaned_role_title, '(?i)(data\\s*scientist|data\\s*science|data\\s*engineer|machine\\s*learning|\\bml\\s+engineer|analytics\\s*engineer|business\\s*intelligence|\\bbi\\s+developer|datenanalyst|datenanalyse|data\\s*analyst|business\\s*analyst.*\\b(data|analytics|bi)\\b|research\\s*scientist.*\\b(data|ml)\\b|etl|data\\s*warehouse|dwh)') THEN 'data_analytics'
            WHEN regexp_like(cleaned_role_title, '(?i)(\\bda\\b|\\bde\\b)\\s*(engineer|developer|analyst)|power\\s*bi|tableau|looker') THEN 'data_analytics'
            -- 3 DevOps / SRE / cloud infra
            WHEN regexp_like(cleaned_role_title, '(?i)(devops|dev\\s*ops|site\\s*reliability|\\bsre\\b|kubernetes|\\bk8s\\b|cloud\\s*engineer|platform\\s*engineer|infrastructure|cloud\\s*architect|terraform|ansible|ci/cd)') THEN 'devops_cloud_infrastructure'
            -- 4 Customer success / support (before generic "engineer")
            WHEN regexp_like(cleaned_role_title, '(?i)(customer\\s*success|customer\\s*support|client\\s*success|help\\s*desk|helpdesk|service\\s*desk|kundenservice|kundenbetreuung|technical\\s*support|it\\s*support|2nd\\s*line|1st\\s*line|support\\s*specialist|support\\s*engineer)') THEN 'customer_success_support'
            -- 5 Software / application development
            WHEN regexp_like(cleaned_role_title, '(?i)(software|entwickler|entwicklung|developer|programmer|programmier|full[\\s-]?stack|front[\\s-]?end|back[\\s-]?end|web[\\s-]?entwickler|softwareingenieur|software[\\s-]?ingenieur|application\\s*engineer|applications\\s*engineer|solutions\\s*engineer|\\bjava\\b|\\bpython\\b|\\b\\.net\\b|\\bphp\\b|react|angular|node\\.js|typescript|mobile\\s*developer|app\\s*developer|embedded\\s*software)') THEN 'software_engineering'
            -- 6 Product / program / project / agile delivery
            WHEN regexp_like(cleaned_role_title, '(?i)(product\\s*owner|product\\s*manager|projektmanager|project\\s*manager|program\\s*manager|programmmanager|scrum\\s*master|agile\\s*coach|delivery\\s*manager|release\\s*train|technical\\s*project)') THEN 'product_program_project'
            -- 7 Marketing / content / growth
            WHEN regexp_like(cleaned_role_title, '(?i)(marketing|growth|content\\s*manager|content\\s*marketing|seo|sem|social\\s*media|brand|kommunikation|communications|copywriter)') THEN 'marketing_content_growth'
            -- 8 Sales / BD / account
            WHEN regexp_like(cleaned_role_title, '(?i)(sales|vertrieb|business\\s*development|\\bbd\\b|account\\s*executive|account\\s*manager|key\\s*account|inside\\s*sales|aussendienst|innendienst|verkauf)') THEN 'sales_business_development'
            -- 9 Finance / accounting / controlling
            WHEN regexp_like(cleaned_role_title, '(?i)(finance|financial|accountant|accounting|buchhaltung|controlling|controller|treasury|audit|tax|finanz)') THEN 'finance_accounting'
            -- 10 HR / recruiting
            WHEN regexp_like(cleaned_role_title, '(?i)(\\bhr\\b|human\\s*resources|recruiter|recruiting|talent\\s*acquisition|people\\s*partner|personal|personalreferent|people\\s*operations)') THEN 'hr_recruiting'
            -- 11 Design / creative / UX
            WHEN regexp_like(cleaned_role_title, '(?i)(ux\\s*design|ui\\s*design|product\\s*design|graphic\\s*design|designer|creative\\s*director|\\bux\\b|\\bui\\b|motion\\s*design)') THEN 'design_creative'
            -- 12 Legal / compliance
            WHEN regexp_like(cleaned_role_title, '(?i)(legal\\s*counsel|corporate\\s*counsel|compliance\\s*officer|\\bjurist\\b|rechtsanwalt|paralegal|legal\\s*advisor)') THEN 'legal_compliance'
            -- 13 Hardware / embedded / electronics (non-software-primary)
            WHEN regexp_like(cleaned_role_title, '(?i)(firmware|hardware\\s*engineer|elektronik|electronics\\s*engineer|semiconductor|halbleiter|pcb|asic|fpga|\\bembedded\\s+systems\\b|\\bembedded\\s+hardware\\b)') THEN 'hardware_electronics_embedded'
            -- 14 Consulting / advisory
            WHEN regexp_like(cleaned_role_title, '(?i)(consultant|consulting|berater|beratung|advisory|professional\\s*services)') THEN 'consulting'
            -- 15 Operations / office / admin
            WHEN regexp_like(cleaned_role_title, '(?i)(operations\\s*manager|office\\s*manager|executive\\s*assistant|administrator|administration|verwaltung|sekretär|facility|einkauf|procurement|logistics|supply\\s*chain)') THEN 'operations_administration'
            ELSE 'unknown_other'
        END AS normalized_role_group
    FROM stripped
)
SELECT
    posted_month,
    run_id,
    raw_role,
    cleaned_role_title,
    normalized_role_group,
    job_count
FROM classified;
```


### `jmi_analytics_v2.v2_eu_employers_top_clean`

**Graph:** QuickSight **employer concentration** (bar / table; top 50 + long tail per month).

**Made:** Gold `company_hiring_monthly`; suffix strip; rank; top 50 vs `Remaining employers (combined)`.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_employers_top_clean AS
WITH month_bounds AS (
    SELECT
        date_format(date_add('month', -1, date_trunc('month', current_timestamp)), '%Y-%m') AS pm_min,
        date_format(date_trunc('month', current_timestamp), '%Y-%m') AS pm_max
),
month_latest AS (
    SELECT r.posted_month, MAX(r.run_id) AS run_id
    FROM jmi_gold_v2.role_demand_monthly r
    CROSS JOIN month_bounds b
    WHERE r.source = 'arbeitnow'
        AND r.posted_month BETWEEN b.pm_min AND b.pm_max
    GROUP BY r.posted_month
),
cleaned AS (
    SELECT
        c.posted_month,
        c.run_id,
        c.job_count,
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(trim(c.company_name)),
                            '(?i)\\s+(gmbh|g\\.m\\.b\\.h\\.|ag|ug|ltd\\.?|llc|inc\\.?|corp\\.?|corporation|s\\.a\\.|s\\.l\\.|bv|plc|kg|ohg|gbr|mbh|co\\.|company)\\.?\\s*$',
                            ''
                        ),
                        '(?i)^the\\s+',
                        ''
                    ),
                    '\\s+',
                    ' '
                ),
                '^[, .;:|\\\\/-]+|[, .;:|\\\\/-]+$',
                ''
            )
        ) AS company_key
    FROM jmi_gold_v2.company_hiring_monthly c
    INNER JOIN month_latest ml ON c.posted_month = ml.posted_month AND c.run_id = ml.run_id
    WHERE c.source = 'arbeitnow'
      AND c.posted_month BETWEEN '2018-01' AND '2035-12'
),
normalized AS (
    SELECT
        posted_month,
        run_id,
        CASE
            WHEN company_key = '' OR company_key IS NULL THEN '(unknown employer)'
            ELSE company_key
        END AS company_key,
        job_count
    FROM cleaned
),
agg AS (
    SELECT
        posted_month,
        run_id,
        company_key,
        SUM(job_count) AS job_count
    FROM normalized
    GROUP BY posted_month, run_id, company_key
),
ranked AS (
    SELECT
        posted_month,
        run_id,
        company_key,
        job_count,
        ROW_NUMBER() OVER (
            PARTITION BY posted_month, run_id
            ORDER BY job_count DESC, company_key ASC
        ) AS rn
    FROM agg
),
rolled AS (
    SELECT
        posted_month,
        run_id,
        CASE
            WHEN rn <= 50 THEN company_key
            ELSE '__LONG_TAIL__'
        END AS company_label,
        SUM(job_count) AS job_count
    FROM ranked
    GROUP BY
        posted_month,
        run_id,
        CASE
            WHEN rn <= 50 THEN company_key
            ELSE '__LONG_TAIL__'
        END
),
labeled AS (
    SELECT
        posted_month,
        run_id,
        company_label,
        job_count,
        CASE
            WHEN company_label = '__LONG_TAIL__' THEN 'Remaining employers (combined)'
            WHEN company_label = '(unknown employer)' THEN 'Unknown employer'
            ELSE regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                array_join(
                                                    transform(
                                                        split(company_label, ' '),
                                                        w -> concat(upper(substr(w, 1, 1)), lower(substr(w, 2)))
                                                    ),
                                                    ' '
                                                ),
                                                '(?i)\.ai$', '.ai'
                                            ),
                                            '(?i)\.io$', '.io'
                                        ),
                                        '(?i)Gmbh', 'GmbH'
                                    ),
                                    '(?i) Se$', ' SE'
                                ),
                                '(?i) Ag$', ' AG'
                            ),
                            '(?i)\s*E\.v\.?$', ' e.V.'
                        ),
                        '(?i)Kg\b', 'KG'
                    ),
                    '(?i)Ug\b', 'UG'
                )
            END AS display_label_raw
    FROM rolled
)
SELECT
    posted_month,
    run_id,
    CASE display_label_raw
        WHEN 'My Humancapital GmbH' THEN 'My Humancapital'
        WHEN 'Sumup' THEN 'SumUp'
        WHEN 'Acemate.ai' THEN 'Acemate'
        WHEN 'United Media' THEN 'United Media'
        WHEN 'Matchingcompany®' THEN 'MatchingCompany'
        WHEN 'Mammaly' THEN 'Mammaly'
        WHEN 'Wolt - English' THEN 'Wolt'
        WHEN 'Accenture' THEN 'Accenture'
        WHEN 'Efly Marketplace Services GmbH' THEN 'Efly Marketplace Services'
        WHEN 'Flix' THEN 'Flix'
        WHEN 'Schwertfels Consulting GmbH' THEN 'Schwertfels Consulting'
        WHEN 'Think About It GmbH' THEN 'Think About It'
        WHEN 'Audius SE' THEN 'Audius'
        WHEN 'Genossenschaftsverband Bayern e.V.' THEN 'Genossenschaftsverband Bayern'
        WHEN 'Solaredge' THEN 'SolarEdge'
        WHEN 'Automat-it' THEN 'Automat'
        WHEN 'Intercon Solutions GmbH' THEN 'Intercon Solutions'
        WHEN 'Prime Hr Agentur®' THEN 'Prime HR Agentur'
        WHEN 'Remmert GmbH' THEN 'Remmert'
        WHEN 'Wavestone Germany AG' THEN 'Wavestone Germany'
        WHEN 'Hm Management Services GmbH' THEN 'HM Management Services'
        WHEN 'Taxtalente.de' THEN 'Taxtalente'
        WHEN 'Ventura Travel' THEN 'Ventura Travel'
        ELSE display_label_raw
    END AS company_label,
    job_count
FROM labeled
WHERE job_count > 0;
```


### `jmi_analytics_v2.v2_in_role_titles_classified`

**Graph:** QuickSight **India classified role families** (bar / combo / family drilldown).

**Made:** Same logic as `role_title_classified_adzuna` in `ATHENA_VIEWS_ADZUNA.sql`; deployed with `jmi_gold_v2` and this view name via `deploy_jmi_analytics_v2_minimal.py`.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_role_titles_classified AS
WITH lr AS (
    SELECT run_id FROM jmi_gold_v2.latest_run_metadata_adzuna LIMIT 1
),
base AS (
    SELECT
        r.posted_month,
        r.run_id,
        r."role" AS raw_role,
        r.job_count,
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        lower(trim(r."role")),
                                        '(?i)(\\(m/w/d\\)|\\(m/f/x\\)|\\(w/m/d\\)|\\(m/f/d\\)|\\(f/m/d\\)|\\(d/m/w\\)|\\(d/f/m\\)|\\(m/w/x\\))',
                                        ' '
                                    ),
                                    '(?i)(all genders|alle\\s+geschlechter|geschlecht\\s*egal)',
                                    ' '
                                ),
                                '(?i)ref\\.?\\s*nr\\.?\\s*:?\\s*[\\w./#-]+',
                                ' '
                            ),
                            '(?i)(job\\s*id\\s*[:#]?\\s*[\\w-]+|stellennr\\.?\\s*:?\\s*[\\w-]+)',
                            ' '
                        ),
                        '(?i)(\\(remote\\)|\\(hybrid\\)|\\(onsite\\)|\\(vor ort\\))',
                        ' '
                    ),
                    '[,;:|/\\\\.-]{2,}',
                    ' '
                ),
                '\\s+',
                ' '
            )
        ) AS c0
    FROM jmi_gold_v2.role_demand_monthly r
    INNER JOIN lr ON r.run_id = lr.run_id
    WHERE r.source = 'adzuna_in'
      AND r.posted_month BETWEEN '2018-01' AND '2035-12'
),
stripped AS (
    SELECT
        posted_month,
        run_id,
        raw_role,
        job_count,
        trim(regexp_replace(regexp_replace(c0, '^[, .;:|\\\\/-]+', ''), '[, .;:|\\\\/-]+$', '')) AS cleaned_role_title
    FROM base
),
classified AS (
    SELECT
        posted_month,
        run_id,
        raw_role,
        job_count,
        CASE
            WHEN cleaned_role_title = '' OR cleaned_role_title IS NULL THEN '(empty title)'
            ELSE cleaned_role_title
        END AS cleaned_role_title,
        CASE
            WHEN cleaned_role_title = '' OR cleaned_role_title IS NULL THEN 'unknown_other'
            -- 1 Cybersecurity (before generic "security" in other contexts if needed — narrow patterns)
            WHEN regexp_like(cleaned_role_title, '(?i)(cyber\\s*security|cybersecurity|informationssicherheit|information security|it[\\s-]*security|pentest|penetration|appsec|soc analyst|security engineer|security architect)') THEN 'cybersecurity'
            -- 2 Data / analytics / ML / BI
            WHEN regexp_like(cleaned_role_title, '(?i)(data\\s*scientist|data\\s*science|data\\s*engineer|machine\\s*learning|\\bml\\s+engineer|analytics\\s*engineer|business\\s*intelligence|\\bbi\\s+developer|datenanalyst|datenanalyse|data\\s*analyst|business\\s*analyst.*\\b(data|analytics|bi)\\b|research\\s*scientist.*\\b(data|ml)\\b|etl|data\\s*warehouse|dwh)') THEN 'data_analytics'
            WHEN regexp_like(cleaned_role_title, '(?i)(\\bda\\b|\\bde\\b)\\s*(engineer|developer|analyst)|power\\s*bi|tableau|looker') THEN 'data_analytics'
            -- 3 DevOps / SRE / cloud infra
            WHEN regexp_like(cleaned_role_title, '(?i)(devops|dev\\s*ops|site\\s*reliability|\\bsre\\b|kubernetes|\\bk8s\\b|cloud\\s*engineer|platform\\s*engineer|infrastructure|cloud\\s*architect|terraform|ansible|ci/cd)') THEN 'devops_cloud_infrastructure'
            -- 4 Customer success / support (before generic "engineer")
            WHEN regexp_like(cleaned_role_title, '(?i)(customer\\s*success|customer\\s*support|client\\s*success|help\\s*desk|helpdesk|service\\s*desk|kundenservice|kundenbetreuung|technical\\s*support|it\\s*support|2nd\\s*line|1st\\s*line|support\\s*specialist|support\\s*engineer)') THEN 'customer_success_support'
            -- 5 Software / application development
            WHEN regexp_like(cleaned_role_title, '(?i)(software|entwickler|entwicklung|developer|programmer|programmier|full[\\s-]?stack|front[\\s-]?end|back[\\s-]?end|web[\\s-]?entwickler|softwareingenieur|software[\\s-]?ingenieur|application\\s*engineer|applications\\s*engineer|solutions\\s*engineer|\\bjava\\b|\\bpython\\b|\\b\\.net\\b|\\bphp\\b|react|angular|node\\.js|typescript|mobile\\s*developer|app\\s*developer|embedded\\s*software)') THEN 'software_engineering'
            -- 6 Product / program / project / agile delivery
            WHEN regexp_like(cleaned_role_title, '(?i)(product\\s*owner|product\\s*manager|projektmanager|project\\s*manager|program\\s*manager|programmmanager|scrum\\s*master|agile\\s*coach|delivery\\s*manager|release\\s*train|technical\\s*project)') THEN 'product_program_project'
            -- 7 Marketing / content / growth
            WHEN regexp_like(cleaned_role_title, '(?i)(marketing|growth|content\\s*manager|content\\s*marketing|seo|sem|social\\s*media|brand|kommunikation|communications|copywriter)') THEN 'marketing_content_growth'
            -- 8 Sales / BD / account
            WHEN regexp_like(cleaned_role_title, '(?i)(sales|vertrieb|business\\s*development|\\bbd\\b|account\\s*executive|account\\s*manager|key\\s*account|inside\\s*sales|aussendienst|innendienst|verkauf)') THEN 'sales_business_development'
            -- 9 Finance / accounting / controlling
            WHEN regexp_like(cleaned_role_title, '(?i)(finance|financial|accountant|accounting|buchhaltung|controlling|controller|treasury|audit|tax|finanz)') THEN 'finance_accounting'
            -- 10 HR / recruiting
            WHEN regexp_like(cleaned_role_title, '(?i)(\\bhr\\b|human\\s*resources|recruiter|recruiting|talent\\s*acquisition|people\\s*partner|personal|personalreferent|people\\s*operations)') THEN 'hr_recruiting'
            -- 11 Design / creative / UX
            WHEN regexp_like(cleaned_role_title, '(?i)(ux\\s*design|ui\\s*design|product\\s*design|graphic\\s*design|designer|creative\\s*director|\\bux\\b|\\bui\\b|motion\\s*design)') THEN 'design_creative'
            -- 12 Legal / compliance
            WHEN regexp_like(cleaned_role_title, '(?i)(legal\\s*counsel|corporate\\s*counsel|compliance\\s*officer|\\bjurist\\b|rechtsanwalt|paralegal|legal\\s*advisor)') THEN 'legal_compliance'
            -- 13 Hardware / embedded / electronics (non-software-primary)
            WHEN regexp_like(cleaned_role_title, '(?i)(firmware|hardware\\s*engineer|elektronik|electronics\\s*engineer|semiconductor|halbleiter|pcb|asic|fpga|\\bembedded\\s+systems\\b|\\bembedded\\s+hardware\\b)') THEN 'hardware_electronics_embedded'
            -- 14 Consulting / advisory
            WHEN regexp_like(cleaned_role_title, '(?i)(consultant|consulting|berater|beratung|advisory|professional\\s*services)') THEN 'consulting'
            -- 15 Operations / office / admin
            WHEN regexp_like(cleaned_role_title, '(?i)(operations\\s*manager|office\\s*manager|executive\\s*assistant|administrator|administration|verwaltung|sekretär|facility|einkauf|procurement|logistics|supply\\s*chain)') THEN 'operations_administration'
            ELSE 'unknown_other'
        END AS normalized_role_group
    FROM stripped
)
SELECT
    posted_month,
    run_id,
    raw_role,
    cleaned_role_title,
    normalized_role_group,
    job_count
FROM classified;;
```


### `jmi_analytics_v2.v2_in_employers_top_clean`

**Graph:** QuickSight **India employers** (bar / concentration; top 50 + long tail at run level).

**Made:** Same as `company_top15_other_clean_adzuna` in `ATHENA_VIEWS_ADZUNA.sql` with `jmi_gold_v2` and view name above.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_employers_top_clean AS
WITH lr AS (
    SELECT run_id FROM jmi_gold_v2.latest_run_metadata_adzuna LIMIT 1
),
cleaned AS (
    SELECT
        c.posted_month,
        c.run_id,
        c.job_count,
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(trim(c.company_name)),
                            '(?i)\\s+(gmbh|g\\.m\\.b\\.h\\.|ag|ug|ltd\\.?|llc|inc\\.?|corp\\.?|corporation|s\\.a\\.|s\\.l\\.|bv|plc|kg|ohg|gbr|mbh|co\\.|company)\\.?\\s*$',
                            ''
                        ),
                        '(?i)^the\\s+',
                        ''
                    ),
                    '\\s+',
                    ' '
                ),
                '^[, .;:|\\\\/-]+|[, .;:|\\\\/-]+$',
                ''
            )
        ) AS company_key
    FROM jmi_gold_v2.company_hiring_monthly c
    INNER JOIN lr ON c.run_id = lr.run_id
    WHERE c.source = 'adzuna_in'
      AND c.posted_month BETWEEN '2018-01' AND '2035-12'
),
normalized AS (
    SELECT
        posted_month,
        run_id,
        CASE
            WHEN company_key = '' OR company_key IS NULL THEN '(unknown employer)'
            ELSE company_key
        END AS company_key,
        job_count
    FROM cleaned
),
agg AS (
    SELECT
        run_id,
        company_key,
        SUM(job_count) AS job_count,
        MAX(posted_month) AS posted_month
    FROM normalized
    GROUP BY run_id, company_key
),
ranked AS (
    SELECT
        posted_month,
        run_id,
        company_key,
        job_count,
        ROW_NUMBER() OVER (
            PARTITION BY run_id
            ORDER BY job_count DESC, company_key ASC
        ) AS rn
    FROM agg
),
rolled AS (
    SELECT
        run_id,
        MAX(posted_month) AS posted_month,
        CASE
            WHEN rn <= 50 THEN company_key
            ELSE '__LONG_TAIL__'
        END AS company_label,
        SUM(job_count) AS job_count
    FROM ranked
    GROUP BY
        run_id,
        CASE
            WHEN rn <= 50 THEN company_key
            ELSE '__LONG_TAIL__'
        END
),
labeled AS (
    SELECT
        posted_month,
        run_id,
        company_label,
        job_count,
        CASE
            WHEN company_label = '__LONG_TAIL__' THEN 'Remaining employers (combined)'
            WHEN company_label = '(unknown employer)' THEN 'Unknown employer'
            ELSE regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                array_join(
                                                    transform(
                                                        split(company_label, ' '),
                                                        w -> concat(upper(substr(w, 1, 1)), lower(substr(w, 2)))
                                                    ),
                                                    ' '
                                                ),
                                                '(?i)\.ai$', '.ai'
                                            ),
                                            '(?i)\.io$', '.io'
                                        ),
                                        '(?i)Gmbh', 'GmbH'
                                    ),
                                    '(?i) Se$', ' SE'
                                ),
                                '(?i) Ag$', ' AG'
                            ),
                            '(?i)\s*E\.v\.?$', ' e.V.'
                        ),
                        '(?i)Kg\b', 'KG'
                    ),
                    '(?i)Ug\b', 'UG'
                )
            END AS display_label_raw
    FROM rolled
)
SELECT
    posted_month,
    run_id,
    CASE display_label_raw
        WHEN 'My Humancapital GmbH' THEN 'My Humancapital'
        WHEN 'Sumup' THEN 'SumUp'
        WHEN 'Acemate.ai' THEN 'Acemate'
        WHEN 'United Media' THEN 'United Media'
        WHEN 'Matchingcompany®' THEN 'MatchingCompany'
        WHEN 'Mammaly' THEN 'Mammaly'
        WHEN 'Wolt - English' THEN 'Wolt'
        WHEN 'Accenture' THEN 'Accenture'
        WHEN 'Efly Marketplace Services GmbH' THEN 'Efly Marketplace Services'
        WHEN 'Flix' THEN 'Flix'
        WHEN 'Schwertfels Consulting GmbH' THEN 'Schwertfels Consulting'
        WHEN 'Think About It GmbH' THEN 'Think About It'
        WHEN 'Audius SE' THEN 'Audius'
        WHEN 'Genossenschaftsverband Bayern e.V.' THEN 'Genossenschaftsverband Bayern'
        WHEN 'Solaredge' THEN 'SolarEdge'
        WHEN 'Automat-it' THEN 'Automat'
        WHEN 'Intercon Solutions GmbH' THEN 'Intercon Solutions'
        WHEN 'Prime Hr Agentur®' THEN 'Prime HR Agentur'
        WHEN 'Remmert GmbH' THEN 'Remmert'
        WHEN 'Wavestone Germany AG' THEN 'Wavestone Germany'
        WHEN 'Hm Management Services GmbH' THEN 'HM Management Services'
        WHEN 'Taxtalente.de' THEN 'Taxtalente'
        WHEN 'Ventura Travel' THEN 'Ventura Travel'
        ELSE display_label_raw
    END AS company_label,
    job_count
FROM labeled
WHERE job_count > 0;;
```


### `jmi_analytics_v2.v2_in_heatmap_state_skill_monthly`

**Graph:** QuickSight **heatmap** (x = India state `dim_x`, y = skill bucket `dim_y`, color/size = `job_count`).

**Made:** Join skills-long to geo rules; top 15 skills per month + Other; `COUNT(DISTINCT job_id)` per cell.

```sql
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
```


### `jmi_analytics_v2.v2_in_sankey_state_to_company_monthly`

**Graph:** QuickSight **Sankey** (source = state, target = employer bucket, weight = `edge_weight`).

**Made:** Geo × Silver base; `ROW_NUMBER` top 10 companies per month + Other; aggregate counts.

```sql
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
```


### `jmi_analytics_v2.v2_in_radar_profile_monthly`

**Graph:** QuickSight **radar chart** (one axis per row: `axis_name` / `axis_value` in [0,1]).

**Made:** Monthly rates + geo top-3 + employer top-5 shares; `UNION ALL` long format.

```sql
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
```


### `jmi_analytics_v2.v2_in_silver_data_coverage_funnel_monthly`

**Graph:** QuickSight **funnel chart** (nested data-coverage stages, not hiring stages).

**Made:** `COUNT_IF` nested filters on Silver base; `UNION ALL` four stages.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_silver_data_coverage_funnel_monthly AS
WITH base AS (
  SELECT *
  FROM jmi_analytics_v2.v2_in_silver_jobs_base
  WHERE source = 'adzuna_in'
),
agg AS (
  SELECT
    source,
    posted_month,
    max(bronze_run_id) AS latest_bronze_run_id,
    COUNT(*) AS c_all,
    COUNT_IF(trim(location_raw) <> '' AND lower(trim(location_raw)) <> 'india') AS c_subnational,
    COUNT_IF(
      trim(location_raw) <> ''
      AND lower(trim(location_raw)) <> 'india'
      AND COALESCE(cardinality(TRY_CAST(json_parse(skills_json) AS array(varchar))), 0) > 0
    ) AS c_subnational_skills,
    COUNT_IF(
      trim(location_raw) <> ''
      AND lower(trim(location_raw)) <> 'india'
      AND COALESCE(cardinality(TRY_CAST(json_parse(skills_json) AS array(varchar))), 0) > 0
      AND lower(trim(remote_type)) <> 'unknown'
    ) AS c_subnational_skills_remote_known
  FROM base
  GROUP BY source, posted_month
)
SELECT
  CAST('data_coverage' AS varchar) AS funnel_kind,
  CAST(1 AS integer) AS stage_order,
  CAST('1_all_postings' AS varchar) AS stage_name,
  CAST('All ingested postings in the merged Silver snapshot' AS varchar) AS stage_description,
  c_all AS job_count,
  source,
  posted_month,
  latest_bronze_run_id AS bronze_run_id
FROM agg
UNION ALL
SELECT
  CAST('data_coverage' AS varchar),
  CAST(2 AS integer),
  CAST('2_subnational_location' AS varchar),
  CAST('Location is more specific than country-only (excludes empty and india-only)' AS varchar),
  c_subnational,
  source,
  posted_month,
  latest_bronze_run_id
FROM agg
UNION ALL
SELECT
  CAST('data_coverage' AS varchar),
  CAST(3 AS integer),
  CAST('3_subnational_with_skills' AS varchar),
  CAST('Subnational location AND at least one extracted skill token' AS varchar),
  c_subnational_skills,
  source,
  posted_month,
  latest_bronze_run_id
FROM agg
UNION ALL
SELECT
  CAST('data_coverage' AS varchar),
  CAST(4 AS integer),
  CAST('4_remote_mode_classified' AS varchar),
  CAST('Subnational + skills AND remote_type is not unknown (hybrid/onsite/remote)' AS varchar),
  c_subnational_skills_remote_known,
  source,
  posted_month,
  latest_bronze_run_id
FROM agg;
```


### `jmi_analytics_v2.v2_eu_location_scatter_metrics`

**Graph:** QuickSight **scatter / bubble** (x = `location_job_count`, y = `location_share_of_monthly_total`).

**Made:** Gold `location_demand_monthly` + `month_latest`; divide by monthly total.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_eu_location_scatter_metrics AS
WITH month_bounds AS (
  SELECT
    date_format(date_add('month', -1, date_trunc('month', current_timestamp)), '%Y-%m') AS pm_min,
    date_format(date_trunc('month', current_timestamp), '%Y-%m') AS pm_max
),
month_latest AS (
  SELECT r.posted_month, MAX(r.run_id) AS run_id
  FROM jmi_gold_v2.role_demand_monthly r
  CROSS JOIN month_bounds b
  WHERE r.source = 'arbeitnow'
    AND r.posted_month BETWEEN b.pm_min AND b.pm_max
  GROUP BY r.posted_month
),
base AS (
  SELECT
    l.location AS location_label,
    l.posted_month,
    l.job_count,
    l.run_id
  FROM jmi_gold_v2.location_demand_monthly l
  INNER JOIN month_latest ml ON l.posted_month = ml.posted_month AND l.run_id = ml.run_id
  WHERE l.source = 'arbeitnow'
),
tot AS (
  SELECT run_id, posted_month, SUM(job_count) AS monthly_total
  FROM base
  GROUP BY run_id, posted_month
)
SELECT
  b.location_label,
  b.posted_month,
  b.job_count AS location_job_count,
  b.run_id,
  CAST(b.job_count AS double) / CAST(NULLIF(t.monthly_total, 0) AS double) AS location_share_of_monthly_total
FROM base b
INNER JOIN tot t ON b.run_id = t.run_id AND b.posted_month = t.posted_month;
```


### `jmi_analytics_v2.v2_eu_sankey_location_to_company_monthly`

**Graph:** QuickSight **Sankey** (location bucket → employer bucket, `edge_weight`).

**Made:** Silver `v2_eu_silver_jobs_base`; top 10 location + top 10 company per month + Other.

```sql
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
```


### `jmi_analytics_v2.v2_cmp_location_hhi_monthly`

**Graph:** QuickSight **line / bar** of location HHI for EU vs IN on strict intersection months.

**Made:** `location_demand_monthly` for both sources; months in both; HHI = sum share^2.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_cmp_location_hhi_monthly AS
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
loc_strict AS (
    SELECT
        l.source,
        l.location,
        l.job_count,
        l.posted_month,
        l.run_id
    FROM jmi_gold_v2.location_demand_monthly l
    INNER JOIN month_latest_eu m ON l.posted_month = m.posted_month AND l.run_id = m.run_id
    CROSS JOIN month_bounds b
    WHERE l.source = 'arbeitnow'
      AND l.posted_month BETWEEN b.pm_min AND b.pm_max
      AND l.posted_month IN (SELECT i.posted_month FROM intersection i)

    UNION ALL

    SELECT
        l.source,
        l.location,
        l.job_count,
        l.posted_month,
        l.run_id
    FROM jmi_gold_v2.location_demand_monthly l
    INNER JOIN month_latest_ad m ON l.posted_month = m.posted_month AND l.run_id = m.run_id
    CROSS JOIN month_bounds b
    WHERE l.source = 'adzuna_in'
      AND l.posted_month BETWEEN b.pm_min AND b.pm_max
      AND l.posted_month IN (SELECT i.posted_month FROM intersection i)
),
totals AS (
    SELECT
        source,
        posted_month,
        run_id,
        CAST(SUM(job_count) AS double) AS total_jobs
    FROM loc_strict
    GROUP BY source, posted_month, run_id
),
hhi AS (
    SELECT
        l.source,
        l.posted_month,
        l.run_id,
        SUM(
            POWER(
                CAST(l.job_count AS double) / NULLIF(t.total_jobs, 0),
                2
            )
        ) AS location_hhi
    FROM loc_strict l
    INNER JOIN totals t
        ON l.source = t.source
        AND l.posted_month = t.posted_month
        AND l.run_id = t.run_id
    GROUP BY l.source, l.posted_month, l.run_id
)
SELECT
    h.posted_month,
    h.source,
    h.location_hhi,
    h.run_id,
    CAST('strict_common_month' AS VARCHAR) AS layer_scope,
    CAST(TRUE AS BOOLEAN) AS month_in_strict_intersection
FROM hhi h;
```


### `jmi_analytics_v2.v2_cmp_skills_per_job_april_2026`

**Graph:** QuickSight **box plot** (skills-per-job distribution for April 2026, EU vs IN).

**Made:** Per-job COUNT(*) on skills-long where posted_month = '2026-04'; UNION ALL EU and India.

```sql
CREATE OR REPLACE VIEW jmi_analytics_v2.v2_cmp_skills_per_job_april_2026 AS
WITH eu AS (
  SELECT
    job_id,
    source,
    posted_month,
    CAST(COUNT(*) AS BIGINT) AS skills_per_job,
    MAX(bronze_run_id) AS run_id,
    CAST('2026-04' AS VARCHAR) AS time_axis
  FROM jmi_analytics_v2.v2_eu_silver_jobs_skills_long
  WHERE posted_month = '2026-04'
    AND source = 'arbeitnow'
    AND trim(COALESCE(skill_token, '')) <> ''
  GROUP BY job_id, source, posted_month
),
in_rows AS (
  SELECT
    job_id,
    source,
    posted_month,
    CAST(COUNT(*) AS BIGINT) AS skills_per_job,
    MAX(bronze_run_id) AS run_id,
    CAST('2026-04' AS VARCHAR) AS time_axis
  FROM jmi_analytics_v2.v2_in_silver_jobs_skills_long
  WHERE posted_month = '2026-04'
    AND source = 'adzuna_in'
    AND trim(COALESCE(skill_token, '')) <> ''
  GROUP BY job_id, source, posted_month
)
SELECT source, job_id, posted_month, skills_per_job, run_id, time_axis FROM eu
UNION ALL
SELECT source, job_id, posted_month, skills_per_job, run_id, time_axis FROM in_rows;
```


### `jmi_analytics_v2.comparison_source_skill_mix_aligned_top20`

**Graph:** QuickSight **side-by-side or stacked bar** of aligned top-20 skill mix (EU vs India).

**Made:** Rolling two months; `MAX(run_id)` per source per month; `strict_intersection_latest_month`; global top 20 skills by combined tag mass; shares within source.

```sql
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
-- -----------------------------------------------------------------------------;
```


### `jmi_analytics_v2.comparison_benchmark_aligned_month`

**Graph:** QuickSight **KPI / benchmark row** (posting volume + skill-tag HHI on aligned strict-intersection month).

**Made:** Role totals from `role_demand_monthly`; join skill-tag HHI on same month/run; `alignment_kind` column.

```sql
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
```
