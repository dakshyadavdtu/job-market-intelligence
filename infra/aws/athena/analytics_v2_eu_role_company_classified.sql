-- Europe (Arbeitnow) Gold-derived role + employer quality views (jmi_analytics_v2).
-- Month/run: rolling previous+current UTC month; MAX(run_id) per posted_month (same policy as v2_eu_kpi_slice_monthly).
-- Employers: top-50 + long-tail is computed per posted_month (not summed across months in one run).

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
