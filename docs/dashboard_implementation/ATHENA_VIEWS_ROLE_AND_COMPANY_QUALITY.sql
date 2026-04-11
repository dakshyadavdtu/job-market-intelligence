-- =============================================================================
-- ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql
-- Additive views for Sheet 1 readability (does NOT replace existing jmi_analytics views).
-- Run in same region/account as jmi_gold after base ATHENA_VIEWS.sql (needs latest_pipeline_run).
-- Engine: Athena engine 3 (Trino SQL).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0) role_title_classified — Raw title → cleaned string → role family (audit grain)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.role_title_classified AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run
),
base AS (
    SELECT
        r.ingest_month,
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
),
stripped AS (
    SELECT
        ingest_month,
        run_id,
        raw_role,
        job_count,
        trim(regexp_replace(regexp_replace(c0, '^[, .;:|\\\\/-]+', ''), '[, .;:|\\\\/-]+$', '')) AS cleaned_role_title
    FROM base
),
classified AS (
    SELECT
        ingest_month,
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
    ingest_month,
    run_id,
    raw_role,
    cleaned_role_title,
    normalized_role_group,
    job_count
FROM classified;

-- -----------------------------------------------------------------------------
-- 1) role_group_demand_monthly — Postings by role family
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.role_group_demand_monthly AS
SELECT
    ingest_month,
    run_id,
    normalized_role_group AS role_group,
    SUM(job_count) AS job_count
FROM jmi_analytics.role_title_classified
GROUP BY ingest_month, run_id, normalized_role_group;

-- -----------------------------------------------------------------------------
-- 2) role_group_top20 — Top 20 families by postings
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.role_group_top20 AS
WITH ranked AS (
    SELECT
        ingest_month,
        run_id,
        role_group,
        job_count,
        ROW_NUMBER() OVER (
            PARTITION BY ingest_month, run_id
            ORDER BY job_count DESC, role_group ASC
        ) AS pareto_rank
    FROM jmi_analytics.role_group_demand_monthly
)
SELECT
    ingest_month,
    run_id,
    role_group,
    job_count,
    pareto_rank
FROM ranked
WHERE pareto_rank <= 20;

-- -----------------------------------------------------------------------------
-- 3) role_group_pareto — Pareto over role families
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.role_group_pareto AS
WITH totals AS (
    SELECT
        ingest_month,
        run_id,
        SUM(job_count) AS total_jobs
    FROM jmi_analytics.role_group_demand_monthly
    GROUP BY ingest_month, run_id
)
SELECT
    g.ingest_month,
    g.run_id,
    g.role_group,
    g.job_count,
    ROW_NUMBER() OVER (
        PARTITION BY g.ingest_month, g.run_id
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
                PARTITION BY g.ingest_month, g.run_id
                ORDER BY g.job_count DESC, g.role_group ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / CAST(t.total_jobs AS DOUBLE)
        ELSE NULL
    END AS cumulative_job_pct
FROM jmi_analytics.role_group_demand_monthly g
INNER JOIN totals t
    ON g.ingest_month = t.ingest_month
    AND g.run_id = t.run_id;

-- -----------------------------------------------------------------------------
-- 4) company_top15_other_clean — Legal-suffix collapse + Top 15 + Other
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW jmi_analytics.company_top15_other_clean AS
WITH lr AS (
    SELECT run_id FROM jmi_analytics.latest_pipeline_run
),
cleaned AS (
    SELECT
        c.ingest_month,
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
),
normalized AS (
    SELECT
        ingest_month,
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
        ingest_month,
        run_id,
        company_key,
        SUM(job_count) AS job_count
    FROM normalized
    GROUP BY ingest_month, run_id, company_key
),
ranked AS (
    SELECT
        ingest_month,
        run_id,
        company_key,
        job_count,
        ROW_NUMBER() OVER (
            PARTITION BY ingest_month, run_id
            ORDER BY job_count DESC, company_key ASC
        ) AS rn
    FROM agg
),
rolled AS (
    SELECT
        ingest_month,
        run_id,
        CASE
            WHEN rn <= 15 THEN company_key
            ELSE 'Other'
        END AS company_label,
        SUM(job_count) AS job_count
    FROM ranked
    GROUP BY
        ingest_month,
        run_id,
        CASE
            WHEN rn <= 15 THEN company_key
            ELSE 'Other'
        END
)
SELECT
    ingest_month,
    run_id,
    company_label,
    job_count
FROM rolled
WHERE job_count > 0;

-- =============================================================================
-- NOTES
-- =============================================================================
-- • role_title_classified: use for viva audit (raw → cleaned → group).
-- • If regexp_like fails on your engine, confirm Athena engine v3.
-- • Tweak keyword lists in classified CTE only; priority = top-to-bottom CASE.
-- • company_top15_other_clean: initcap() omitted to avoid odd casing;
--   format labels in QuickSight if desired.
-- =============================================================================