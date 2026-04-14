-- Adzuna India geo helpers (jmi_analytics_v2 only).
-- Depends on: jmi_analytics_v2.v2_in_silver_jobs_base
--
-- State extraction: rule-based (India strings from Silver), not geocoding.
-- City lat/lon: approximate centroids (2 decimal places); geocode_method = approximate_centroid_2dp.
--
-- QuickSight (required — Athena cannot set this): GEOSPATIAL types and the lat/lon HIERARCHY are created
-- in the DATASET EDITOR only. See docs/dashboard_implementation/QUICKSIGHT_INDIA_GEO_HIERARCHY.md
-- (create hierarchy e.g. "Coordinates" containing latitude + longitude, then drag that hierarchy to GEOSPATIAL).
--
-- Filled map (v2_in_geo_state_monthly): country -> Country; state_geo -> State or region (filter NULL).
-- state_geo is NULL when india_state_name is unmapped_* so map binding is not polluted.

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
