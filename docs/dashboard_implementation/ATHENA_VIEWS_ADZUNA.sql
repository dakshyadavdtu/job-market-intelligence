-- =============================================================================
-- ATHENA_VIEWS_ADZUNA.sql — Adzuna-only analytics slice (separate from Arbeitnow latest_pipeline_run)
-- Prerequisites: jmi_gold tables + Adzuna run_ids in Glue projection.run_id.values
-- Engine: Athena engine 3. Do not alter jmi_analytics.latest_pipeline_run or jmi_gold.latest_run_metadata.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS jmi_analytics;

-- -----------------------------------------------------------------------------
-- 0) latest_pipeline_run_adzuna — Newest Adzuna pipeline run (by posted_month, run_id)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.latest_pipeline_run_adzuna AS
SELECT run_id FROM jmi_gold.latest_run_metadata_adzuna LIMIT 1;


-- -----------------------------------------------------------------------------
-- Raw-grain + summary — latest Adzuna run only
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.skill_demand_monthly_adzuna_latest AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna
)
SELECT
    s.skill,
    s.job_count,
    s.source,
    s.bronze_ingest_date,
    s.bronze_run_id,
    s.posted_month,
    s.run_id
FROM jmi_gold.skill_demand_monthly s
INNER JOIN lr ON s.run_id = lr.run_id
WHERE s.source = 'adzuna_in'
  AND s.posted_month BETWEEN '2018-01' AND '2035-12';

CREATE OR REPLACE VIEW jmi_analytics.pipeline_run_summary_adzuna_latest AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna
)
SELECT
    p.source,
    p.bronze_ingest_date,
    p.bronze_run_id,
    p.skill_row_count,
    p.role_row_count,
    p.location_row_count,
    p.company_row_count,
    p.status,
    p.posted_month,
    p.run_id
FROM jmi_gold.pipeline_run_summary p
INNER JOIN lr ON p.run_id = lr.run_id
WHERE p.source = 'adzuna_in'
  AND p.posted_month BETWEEN '2018-01' AND '2035-12';

-- -----------------------------------------------------------------------------
-- location_top15_other_adzuna
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.location_top15_other_adzuna AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna
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
    SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna
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
-- 2) role_group_top20_adzuna — Top 20 families by postings
--     One row per (run_id, role_group): sums all posted_month for latest run only.
--     (Partitioning only by month+run duplicated the same family across months.)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.role_group_top20_adzuna AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna
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
ranked AS (
    SELECT
        posted_month,
        run_id,
        role_group,
        job_count,
        ROW_NUMBER() OVER (
            PARTITION BY run_id
            ORDER BY job_count DESC, role_group ASC
        ) AS pareto_rank
    FROM agg
)
SELECT
    posted_month,
    run_id,
    role_group,
    job_count,
    pareto_rank
FROM ranked
WHERE pareto_rank <= 20;

-- -----------------------------------------------------------------------------
-- 3) role_group_pareto_adzuna — Pareto over role families (latest run; all months summed)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.role_group_pareto_adzuna AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna
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
    SELECT run_id FROM jmi_analytics.latest_pipeline_run_adzuna
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
