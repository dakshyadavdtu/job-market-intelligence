-- =============================================================================
-- ATHENA_VIEWS_ADZUNA.sql — Adzuna-only analytics slice (India / Adzuna run_id via latest_run_metadata_adzuna)
-- Prerequisites: jmi_gold tables + Adzuna run_ids in Glue projection.run_id.values
-- Engine: Athena engine 3.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS jmi_analytics;

-- Raw facts: filter jmi_gold.* by run_id = (SELECT run_id FROM jmi_gold.latest_run_metadata_adzuna LIMIT 1).

-- -----------------------------------------------------------------------------
-- location_top15_other_adzuna
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.location_top15_other_adzuna AS
WITH lr AS (
    SELECT run_id FROM jmi_gold.latest_run_metadata_adzuna LIMIT 1
),
base AS (
    SELECT
        l.posted_month,
        l.run_id,
        l.location,
        l.job_count
    FROM jmi_gold.location_demand_monthly l
    INNER JOIN lr ON l.run_id = lr.run_id
    WHERE l.source = 'adzuna_in'
      AND l.posted_month BETWEEN '2018-01' AND '2035-12'
),
agg AS (
    SELECT
        run_id,
        location,
        SUM(job_count) AS job_count,
        MAX(posted_month) AS posted_month
    FROM base
    GROUP BY run_id, location
),
ranked AS (
    SELECT
        posted_month,
        run_id,
        location,
        job_count,
        ROW_NUMBER() OVER (
            PARTITION BY run_id
            ORDER BY job_count DESC, location ASC
        ) AS rn
    FROM agg
),
rolled AS (
    SELECT
        run_id,
        MAX(posted_month) AS posted_month,
        CASE
            WHEN rn <= 15 THEN location
            ELSE 'Other'
        END AS location_label,
        SUM(job_count) AS job_count
    FROM ranked
    GROUP BY
        run_id,
        CASE
            WHEN rn <= 15 THEN location
            ELSE 'Other'
        END
)
SELECT
    posted_month,
    run_id,
    location_label,
    job_count
FROM rolled
WHERE job_count > 0;

CREATE OR REPLACE VIEW jmi_analytics.role_title_classified_adzuna AS
WITH lr AS (
    SELECT run_id FROM jmi_gold.latest_run_metadata_adzuna LIMIT 1
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
    FROM jmi_gold.role_demand_monthly r
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
FROM classified;

-- -----------------------------------------------------------------------------
-- 1) role_group_demand_monthly_adzuna — Postings by role family
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.role_group_demand_monthly_adzuna AS
SELECT
    posted_month,
    run_id,
    normalized_role_group AS role_group,
    SUM(job_count) AS job_count
FROM jmi_analytics.role_title_classified_adzuna
GROUP BY posted_month, run_id, normalized_role_group;

-- -----------------------------------------------------------------------------
-- 2) role_group_pareto_adzuna — Pareto over role families (latest run; all months summed)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.role_group_pareto_adzuna AS
WITH lr AS (
    SELECT run_id FROM jmi_gold.latest_run_metadata_adzuna LIMIT 1
),
agg AS (
    SELECT
        r.run_id,
        r.role_group,
        SUM(r.job_count) AS job_count,
        MAX(r.posted_month) AS posted_month
    FROM jmi_analytics.role_group_demand_monthly_adzuna r
    INNER JOIN lr ON r.run_id = lr.run_id
    GROUP BY r.run_id, r.role_group
),
totals AS (
    SELECT
        run_id,
        SUM(job_count) AS total_jobs
    FROM agg
    GROUP BY run_id
)
SELECT
    g.posted_month,
    g.run_id,
    g.role_group,
    g.job_count,
    ROW_NUMBER() OVER (
        PARTITION BY g.run_id
        ORDER BY g.job_count DESC, g.role_group ASC
    ) AS pareto_rank,
    CASE
        WHEN t.total_jobs > 0
            THEN CAST(g.job_count AS DOUBLE) / CAST(t.total_jobs AS DOUBLE)
        ELSE NULL
    END AS share_of_total,
    CASE
        WHEN t.total_jobs > 0
            THEN 100.0 * SUM(g.job_count) OVER (
                PARTITION BY g.run_id
                ORDER BY g.job_count DESC, g.role_group ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / CAST(t.total_jobs AS DOUBLE)
        ELSE NULL
    END AS cumulative_job_pct
FROM agg g
INNER JOIN totals t ON g.run_id = t.run_id;

-- -----------------------------------------------------------------------------
-- 4) company_top15_other_clean_adzuna — Legal-suffix collapse + Top 50 + long-tail bucket
--     Run-level totals (all posted_month summed for latest run) before ranking.
--     Display labels: word-level casing + suffix polish (GmbH, SE, AG, e.V., …); TLD .ai lower.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.company_top15_other_clean_adzuna AS
WITH lr AS (
    SELECT run_id FROM jmi_gold.latest_run_metadata_adzuna LIMIT 1
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
    FROM jmi_gold.company_hiring_monthly c
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
WHERE job_count > 0;

-- -----------------------------------------------------------------------------
-- sheet1_kpis_adzuna — India (adzuna_in) KPI row per (posted_month, run_id)
--     Latest Adzuna pipeline run only. Mirrors EU sheet1_kpis: totals, location
--     top-3 share, location HHI, company HHI, top-1 role share.
--     Also: active_posted_months (distinct months in run), top1_location_share,
--     distinct_location_buckets, distinct_role_title_buckets, distinct_role_groups.
--     Plus: distinct_skill_tags, skill_tag_hhi (tag-demand concentration),
--     unknown_role_group_share (classified titles still in unknown_other).
--     Remote/hybrid: NOT included — not in Gold; use structural KPIs above instead.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW jmi_analytics.sheet1_kpis_adzuna AS
WITH
lr AS (
    SELECT run_id FROM jmi_gold.latest_run_metadata_adzuna LIMIT 1
),
run_months AS (
    SELECT
        r.run_id,
        CAST(COUNT(DISTINCT r.posted_month) AS BIGINT) AS active_posted_months
    FROM jmi_gold.role_demand_monthly r
    INNER JOIN lr ON r.run_id = lr.run_id
    WHERE r.source = 'adzuna_in'
      AND r.posted_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY r.run_id
),
role_totals AS (
    SELECT
        r.posted_month,
        r.run_id,
        SUM(r.job_count) AS total_postings,
        MAX(r.job_count) AS max_role_job_count
    FROM jmi_gold.role_demand_monthly r
    INNER JOIN lr ON r.run_id = lr.run_id
    WHERE r.source = 'adzuna_in'
      AND r.posted_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY r.posted_month, r.run_id
),
loc_totals AS (
    SELECT
        l.posted_month,
        l.run_id,
        SUM(l.job_count) AS located_postings
    FROM jmi_gold.location_demand_monthly l
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
        FROM jmi_gold.location_demand_monthly l
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
    FROM jmi_gold.location_demand_monthly l
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
                CAST(l.job_count AS DOUBLE) / CAST(lt.located_postings AS DOUBLE),
                2
            )
        ) AS location_hhi
    FROM jmi_gold.location_demand_monthly l
    INNER JOIN lr ON l.run_id = lr.run_id
    INNER JOIN loc_totals lt
        ON l.posted_month = lt.posted_month
        AND l.run_id = lt.run_id
    WHERE lt.located_postings > 0
        AND l.source = 'adzuna_in'
        AND l.posted_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY l.posted_month, l.run_id
),
comp_totals AS (
    SELECT
        c.posted_month,
        c.run_id,
        SUM(c.job_count) AS company_postings_sum
    FROM jmi_gold.company_hiring_monthly c
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
                CAST(c.job_count AS DOUBLE) / CAST(ct.company_postings_sum AS DOUBLE),
                2
            )
        ) AS company_hhi
    FROM jmi_gold.company_hiring_monthly c
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
        CAST(COUNT(*) AS BIGINT) AS distinct_location_buckets
    FROM jmi_gold.location_demand_monthly l
    INNER JOIN lr ON l.run_id = lr.run_id
    WHERE l.source = 'adzuna_in'
      AND l.posted_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY l.posted_month, l.run_id
),
role_title_buckets AS (
    SELECT
        r.posted_month,
        r.run_id,
        CAST(COUNT(*) AS BIGINT) AS distinct_role_title_buckets
    FROM jmi_gold.role_demand_monthly r
    INNER JOIN lr ON r.run_id = lr.run_id
    WHERE r.source = 'adzuna_in'
      AND r.posted_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY r.posted_month, r.run_id
),
rg_distinct AS (
    SELECT
        rg.posted_month,
        rg.run_id,
        CAST(COUNT(DISTINCT rg.role_group) AS BIGINT) AS distinct_role_groups
    FROM jmi_analytics.role_group_demand_monthly_adzuna rg
    INNER JOIN lr ON rg.run_id = lr.run_id
    WHERE rg.posted_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY rg.posted_month, rg.run_id
),
skill_tag_totals AS (
    SELECT
        s.posted_month,
        s.run_id,
        SUM(s.job_count) AS tag_sum_total
    FROM jmi_gold.skill_demand_monthly s
    INNER JOIN lr ON s.run_id = lr.run_id
    WHERE s.source = 'adzuna_in'
      AND s.posted_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY s.posted_month, s.run_id
),
skill_distinct AS (
    SELECT
        s.posted_month,
        s.run_id,
        CAST(COUNT(DISTINCT s.skill) AS BIGINT) AS distinct_skill_tags
    FROM jmi_gold.skill_demand_monthly s
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
                CAST(s.job_count AS DOUBLE) / CAST(NULLIF(t.tag_sum_total, 0) AS DOUBLE),
                2
            )
        ) AS skill_tag_hhi
    FROM jmi_gold.skill_demand_monthly s
    INNER JOIN lr ON s.run_id = lr.run_id
    INNER JOIN skill_tag_totals t
        ON s.posted_month = t.posted_month
        AND s.run_id = t.run_id
    WHERE s.source = 'adzuna_in'
      AND s.posted_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY s.posted_month, s.run_id
),
unknown_role AS (
    SELECT
        rg.posted_month,
        rg.run_id,
        SUM(
            CASE
                WHEN rg.role_group = 'unknown_other' THEN rg.job_count
                ELSE CAST(0 AS BIGINT)
            END
        ) AS unknown_role_jobs,
        SUM(rg.job_count) AS role_group_postings
    FROM jmi_analytics.role_group_demand_monthly_adzuna rg
    INNER JOIN lr ON rg.run_id = lr.run_id
    WHERE rg.posted_month BETWEEN '2018-01' AND '2035-12'
    GROUP BY rg.posted_month, rg.run_id
)
SELECT
    r.posted_month,
    r.run_id,
    rm.active_posted_months,
    r.total_postings,
    COALESCE(l.located_postings, CAST(0 AS BIGINT)) AS located_postings,
    CASE
        WHEN COALESCE(l.located_postings, 0) > 0
            THEN CAST(COALESCE(t3.top3_location_job_sum, 0) AS DOUBLE)
                / CAST(l.located_postings AS DOUBLE)
        ELSE NULL
    END AS top3_location_share,
    CASE
        WHEN COALESCE(l.located_postings, 0) > 0 AND lm.max_location_job_count IS NOT NULL
            THEN CAST(lm.max_location_job_count AS DOUBLE) / CAST(l.located_postings AS DOUBLE)
        ELSE NULL
    END AS top1_location_share,
    lh.location_hhi AS location_hhi,
    ch.company_hhi AS company_hhi,
    CASE
        WHEN r.total_postings > 0
            THEN CAST(r.max_role_job_count AS DOUBLE) / CAST(r.total_postings AS DOUBLE)
        ELSE NULL
    END AS top1_role_share,
    lb.distinct_location_buckets,
    rtb.distinct_role_title_buckets,
    rg.distinct_role_groups,
    sd.distinct_skill_tags,
    sh.skill_tag_hhi,
    CASE
        WHEN COALESCE(ur.role_group_postings, 0) > 0
            THEN CAST(ur.unknown_role_jobs AS DOUBLE) / CAST(ur.role_group_postings AS DOUBLE)
        ELSE NULL
    END AS unknown_role_group_share
FROM role_totals r
INNER JOIN run_months rm ON r.run_id = rm.run_id
LEFT JOIN loc_totals l
    ON r.posted_month = l.posted_month AND r.run_id = l.run_id
LEFT JOIN loc_top3 t3
    ON r.posted_month = t3.posted_month AND r.run_id = t3.run_id
LEFT JOIN loc_max lm
    ON r.posted_month = lm.posted_month AND r.run_id = lm.run_id
LEFT JOIN loc_hhi_calc lh
    ON r.posted_month = lh.posted_month AND r.run_id = lh.run_id
LEFT JOIN comp_hhi_calc ch
    ON r.posted_month = ch.posted_month AND r.run_id = ch.run_id
LEFT JOIN loc_buckets lb
    ON r.posted_month = lb.posted_month AND r.run_id = lb.run_id
LEFT JOIN role_title_buckets rtb
    ON r.posted_month = rtb.posted_month AND r.run_id = rtb.run_id
LEFT JOIN rg_distinct rg
    ON r.posted_month = rg.posted_month AND r.run_id = rg.run_id
LEFT JOIN skill_distinct sd
    ON r.posted_month = sd.posted_month AND r.run_id = sd.run_id
LEFT JOIN skill_hhi_calc sh
    ON r.posted_month = sh.posted_month AND r.run_id = sh.run_id
LEFT JOIN unknown_role ur
    ON r.posted_month = ur.posted_month AND r.run_id = ur.run_id;
