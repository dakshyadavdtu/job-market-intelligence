-- Adzuna India geo helpers (jmi_analytics_v2 only).
-- Depends on: jmi_analytics_v2.v2_in_silver_jobs_base
--
-- State extraction: rule-based (India strings from Silver), not geocoding.
-- City lat/lon: approximate centroids (2 decimal places); geocode_method = approximate_centroid_2dp.

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
  CAST(TRIM(state_name_display) AS varchar) AS state_geo,
  CAST('India' AS varchar) AS country,
  job_count
FROM agg;

CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_geo_city_points_monthly AS
WITH ruled AS (
  SELECT
    job_id,
    source,
    posted_month,
    bronze_run_id,
    loc,
    trim(split_part(loc, ',', 1)) AS p1,
    trim(split_part(loc, ',', 2)) AS p2,
    india_state_name
  FROM jmi_analytics_v2.v2_in_geo_location_rules
),
city_pick AS (
  SELECT
    job_id,
    source,
    posted_month,
    bronze_run_id,
    loc,
    india_state_name,
    CASE
      WHEN loc = 'india' OR loc = '' THEN CAST(null AS varchar)
      WHEN p2 = '' AND p1 <> 'india' THEN p1
      WHEN p2 IN ('india', 'in') THEN p1
      WHEN p2 <> '' AND india_state_name NOT LIKE 'unmapped%' THEN p1
      ELSE CAST(null AS varchar)
    END AS city_label
  FROM ruled
),
latlon AS (
  SELECT
    job_id,
    source,
    posted_month,
    bronze_run_id,
    loc,
    india_state_name,
    city_label,
    CASE lower(trim(city_label))
      WHEN 'bangalore' THEN 12.97
      WHEN 'bengaluru' THEN 12.97
      WHEN 'mumbai' THEN 19.08
      WHEN 'hyderabad' THEN 17.39
      WHEN 'chennai' THEN 13.08
      WHEN 'navi mumbai' THEN 19.03
      WHEN 'thane' THEN 19.22
      WHEN 'pune' THEN 18.52
      WHEN 'nashik' THEN 19.99
      WHEN 'new delhi' THEN 28.61
      WHEN 'delhi' THEN 28.61
      WHEN 'noida' THEN 28.54
      WHEN 'kolkata' THEN 22.57
      WHEN 'ahmedabad' THEN 23.03
      WHEN 'jaipur' THEN 26.92
      WHEN 'indore' THEN 22.72
      WHEN 'kochi' THEN 9.93
      WHEN 'coimbatore' THEN 11.02
      WHEN 'vadodara' THEN 22.31
      ELSE CAST(null AS double)
    END AS approx_lat,
    CASE lower(trim(city_label))
      WHEN 'bangalore' THEN 77.59
      WHEN 'bengaluru' THEN 77.59
      WHEN 'mumbai' THEN 72.88
      WHEN 'hyderabad' THEN 78.49
      WHEN 'chennai' THEN 80.28
      WHEN 'navi mumbai' THEN 73.02
      WHEN 'thane' THEN 72.97
      WHEN 'pune' THEN 73.86
      WHEN 'nashik' THEN 73.79
      WHEN 'new delhi' THEN 77.21
      WHEN 'delhi' THEN 77.21
      WHEN 'noida' THEN 77.39
      WHEN 'kolkata' THEN 88.36
      WHEN 'ahmedabad' THEN 72.58
      WHEN 'jaipur' THEN 75.79
      WHEN 'indore' THEN 75.86
      WHEN 'kochi' THEN 76.27
      WHEN 'coimbatore' THEN 76.96
      WHEN 'vadodara' THEN 73.18
      ELSE CAST(null AS double)
    END AS approx_lon
  FROM city_pick
)
SELECT
  source,
  posted_month,
  max(bronze_run_id) AS latest_bronze_run_id,
  lower(trim(city_label)) AS city_key,
  trim(city_label) AS city_label_display,
  india_state_name AS state_name,
  CAST(approx_lat AS double) AS latitude,
  CAST(approx_lon AS double) AS longitude,
  max(CAST('approximate_centroid_2dp' AS varchar)) AS geocode_method,
  CAST(COUNT(*) AS bigint) AS job_count
FROM latlon
WHERE city_label IS NOT NULL
  AND approx_lat IS NOT NULL
  AND approx_lon IS NOT NULL
GROUP BY source, posted_month, lower(trim(city_label)), trim(city_label), india_state_name, approx_lat, approx_lon;
