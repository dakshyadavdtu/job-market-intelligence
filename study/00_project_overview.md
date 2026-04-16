# JMI — Project overview (deep study note, v2 / active state)

**Audience:** Personal study and viva prep.  
**Voice:** Written as if *you* stood up the lakehouse on AWS mostly through the console—clicking Glue/Athena, pasting DDL, wiring Lambda in ECR, and building QuickSight—while this repo is the **source of truth for code and SQL**.  
**Scope:** **Current active v2** only (`jmi_silver_v2`, `jmi_gold_v2`, `jmi_analytics_v2`). Legacy `jmi_gold` / `jmi_analytics` appears only where contrast helps.  
**Primary BI outcome:** The QuickSight dashboard **`dea final 9`** (name is user-facing in QuickSight; exact dataset→visual wiring is **not** exported in git—see §8 and “Uncertainty”).

---

## 1. What this project is in the simplest possible words

It is a **job-market intelligence** system: it regularly pulls job ads from public APIs, keeps a **faithful raw copy**, turns that into **clean one-row-per-job tables**, then builds **small monthly summary tables** (skills, roles, places, employers). Those summaries live in **S3 as Parquet**, are registered in **AWS Glue**, queried with **Athena**, and shown in **QuickSight**—so you can answer “what’s in demand, where, and for whom?” without re-scanning millions of JSON lines every time.

---

## 2. Slightly technical version of the same explanation

**Job Market Intelligence (JMI)** implements a **medallion lakehouse** on **Amazon S3**: **Bronze** (compressed JSON lines + lineage metadata), **Silver** (typed, deduplicated **Parquet** job rows per `source=`), **Gold** (aggregated **Parquet** facts partitioned by **`source`**, **`posted_month`** (calendar month of the posting), and **`run_id`**). The **Glue Data Catalog** holds **external tables** for Gold (and Silver where needed) under databases **`jmi_gold_v2`** and **`jmi_silver_v2`**. **`jmi_analytics_v2`** is almost entirely **views**—KPI slices, EU/India helpers, comparison math—so BI semantics can evolve without rewriting Parquet. **Amazon Athena** runs SQL over those objects; **Amazon QuickSight** (your capstone-facing asset: **`dea final 9`**) consumes Athena datasets. **AWS Lambda** (container image from this repo) can run the same Python entrypoints as your laptop: ingest → silver → gold chain.

---

## 3. What final output this project produces

**Primary (what you defend in a viva):**

- **QuickSight dashboard `dea final 9`:** Multi-sheet analytics over **EU (Arbeitnow)** and **India (Adzuna)** plus **comparison** visuals, backed by **`jmi_analytics_v2`** views (and sometimes direct **`jmi_gold_v2`** fact tables where a view would be redundant). This is the **packaged “so what”** for teachers: KPIs, distributions, maps/flows where helpers exist, and **pipeline proof** rows.
- **Curated Athena surface:** Databases **`jmi_gold_v2`**, **`jmi_silver_v2`**, **`jmi_analytics_v2`**—queryable contracts for the same story without QuickSight.

**Secondary (engineering truth, not the teacher’s first screen):**

- **S3 objects** under a single bucket (DDL examples pin `s3://jmi-dakshyadav-job-market-intelligence/…`—treat bucket name as **your** deployed bucket; verify in console if your account differs) with predictable prefixes: `bronze/`, `silver/jobs/…`, `gold/<table>/…`, `gold/source=<slug>/latest_run_metadata/`, plus `quality/`, `health/`, `athena-results/` by convention.
- **Local Streamlit** (`dashboard/app.py`) for dev/demo over Parquet under `data/`—useful, but **not** the named “final 9” outcome unless you explicitly frame it as a local mirror.

---

## 4. Why this project exists

Raw job feeds are **public signals** that students, mentors, and programs care about—but they are unusable as “insight” until someone **standardizes** titles/skills/locations and **aggregates** to a stable time grain. This project exists to demonstrate **analytics engineering**: auditable ingestion, repeatable transforms, **cost-aware** SQL on a lake, and a **presentation layer** (QuickSight) that non-SQL stakeholders can trust. It is also a **portfolio artifact**: end-to-end from API to dashboard with clear **lineage** (`run_id`, Bronze file pointers).

---

## 5. What problem raw job data has

Specific to **this** codebase’s assumptions:

- **Different JSON shapes per vendor:** Arbeitnow vs Adzuna use different field names and nesting; without a **Bronze envelope** (`raw_payload` + metadata), you cannot replay Silver when mapping rules change.
- **Noise and synonymy:** “React” vs “react.js”; messy location strings; employer legal suffixes—if you aggregate directly from raw text, **charts lie in inconsistent ways**.
- **Duplication:** The same posting can appear across pages or runs; JMI uses a **deterministic `job_id`** and Silver dedupe so **monthly counts are not inflated**.
- **Fast change / pagination:** You need **batch lineage** (`run_id`, `bronze_ingest_date`) to know *which fetch* produced *which row*.
- **Cost of naive querying:** Scanning raw JSONL at BI frequency is **slow and expensive** in Athena; **Gold** exists so standard questions hit **small Parquet files** with partition filters.

---

## 6. Why this needed a pipeline instead of a simple script/dashboard

A **one-off script** that prints CSV from an API fails the moment you need:

- **Auditability:** “Show me exactly what the API returned last Tuesday” → needs **Bronze**, not just a cleaned table.
- **Evolving rules:** Skill extraction and role bucketing **will** change; without **immutable Bronze**, you cannot **rebuild Silver/Gold** honestly.
- **Separation of compute:** **Ingest** (network-bound), **normalize** (CPU-bound), **aggregate** (month-sliced) have different frequencies and failure modes; chaining them as **separate stages** (and in AWS, separate Lambda invocations) keeps retries and costs understandable.
- **Multi-source truth:** EU and India are different `source=` partitions sharing one **Silver schema** and one **Gold fact shape**—a single ad-hoc notebook tends to **mix sources** or hard-code paths; the pipeline enforces **contracts** (`src/jmi/paths.py`, transforms).

So: not “a pipeline because AWS”; a pipeline because **lineage + replay + multi-source + aggregate performance** are first-class requirements.

---

## 7. End-to-end system story: source API → Bronze → Silver → Gold → Glue → Athena → QuickSight

**Narrative you can walk on a whiteboard (v2):**

1. **Source API (HTTP):** **Arbeitnow** (EU) is the **scheduled** default in Lambda (`ingest_live` via `ingest_handler`). **Adzuna India** is fully implemented in **`src/jmi/`** and ships in the **same container image**, but the **default scheduled handler** still calls **Arbeitnow only** unless you add another function/schedule—see `infra/aws/lambda/README.md`. *You* may run Adzuna on a laptop or a separate manual Lambda; that operational choice is **account-specific** (see Uncertainty).
2. **Bronze:** Connector writes **`raw.jsonl.gz`** under `bronze/source=<slug>/ingest_date=…/run_id=…/`. Each line wraps vendor JSON in a **stable envelope** with lineage fields—**no business cleaning** here.
3. **Silver:** `transform_silver` flattens to a **strict Parquet schema**, runs **rule-based skill extraction** (`skill_extract.py`), dedupes on **`job_id`**, writes batch Parquet and typically **`merged/latest.parquet`** for downstream use. Paths are **`silver/jobs/source=<slug>/…`** (active layout).
4. **Gold:** `transform_gold` builds **monthly** aggregates keyed by **`posted_month`** (from posting time, **not** “ingest month”), emitting tables such as **`skill_demand_monthly`**, **`role_demand_monthly`**, **`location_demand_monthly`**, **`company_hiring_monthly`**, plus **`pipeline_run_summary`** and per-source **`latest_run_metadata`** pointers under `gold/source=<slug>/latest_run_metadata/`.
5. **Glue Data Catalog:** **You** (as in this study narrative) created **databases** `jmi_gold_v2`, `jmi_silver_v2`, `jmi_analytics_v2` and ran **DDL from the repo**—e.g. `infra/aws/athena/ddl_gold_*.sql` (patched to `_v2` by `scripts/deploy_athena_v2.py`), Silver merged tables like `ddl_silver_v2_arbeitnow_merged.sql`, and the **`analytics_v2_*.sql`** view families. Tables use **partition projection** (`TBLPROPERTIES` with `projection.*`) so new `run_id` values require **Glue table property updates** (enum lists)—operational coupling you should be able to explain.
6. **Athena:** Workgroup queries hit **external tables** and **views**. Views in **`jmi_analytics_v2`** implement “latest run” filtering, KPI compositions, EU/India helpers (heatmap, sankey, geo, etc.), and **comparison** logic (`ATHENA_VIEWS_COMPARISON_V2.sql` lineage).
7. **QuickSight:** An Athena **data source** connects to the same account/region; **datasets** point at `jmi_analytics_v2` views (naming convention in `QUICKSIGHT_V1_V2_NAMING.md`). The published dashboard **`dea final 9`** is the **capstone-facing** assembly of those datasets into visuals. **Exact mapping** is in the QuickSight UI, not in git.

---

## 8. What is currently active and real in this project

**Data plane (intended production shape):**

- **S3:** Medallion prefixes with **`source=`** segments; Gold facts use **`posted_month=`** partitions on disk (see `src/jmi/paths.py` and `ddl_glue_*` comments). Example bucket referenced in DDL: **`jmi-dakshyadav-job-market-intelligence`**—**confirm** your live bucket in S3 console.
- **Glue / Athena v2 databases:**
  - **`jmi_gold_v2`:** **Physical** external tables over Gold Parquet + **metadata pointer** tables such as **`latest_run_metadata_arbeitnow`** / **`latest_run_metadata_adzuna`** (deploy scripts patch names from `ddl_gold_latest_run_metadata_*.sql`).
  - **`jmi_silver_v2`:** External tables for **merged Silver** (e.g. **`arbeitnow_jobs_merged`** → `silver/jobs/source=arbeitnow/merged/`) used by **Silver-backed analytics** (EU foundation in `analytics_v2_eu_silver_foundation.sql`). Adzuna merged DDL exists as **`ddl_silver_v2_adzuna_merged.sql`** in-repo.
  - **`jmi_analytics_v2`:** **Views** (KPIs, pareto/clean company rollups, India helpers, comparison views). Deploy orchestration: `scripts/deploy_athena_v2.py` + focused deploy scripts under `scripts/deploy_*_v2.py` and `scripts/athena_smoke_v2.py` for smoke queries.

**Compute / automation:**

- **Lambda (container):** `infra/aws/lambda/` — **ingest** handler invokes **Silver** asynchronously, which invokes **Gold** (pattern in `ingest_handler.py` / silver/gold handlers). CI builds and pushes the image (see `.github/workflows/jmi-lambda-ecr-deploy.yml`).
- **EventBridge:** Schedule JSON exists under `infra/aws/eventbridge/`; **docs/checkpoints** note the schedule may be **intentionally disabled** until validation—**verify live** whether your account has auto-runs enabled.

**Repo “deploy truth”:**

- **`scripts/deploy_athena_v2.py`:** Creates `jmi_gold_v2`, runs Gold DDL (with `jmi_gold` → `jmi_gold_v2` rewrite), sets **`projection.run_id.values`** from CLI, then deploys minimal + comparison analytics via subprocesses.
- **`scripts/quicksight_create_datasets_v2.py`:** Example automation to create QuickSight datasets against `jmi_analytics_v2` (embeds **account/region/data source ARNs**—treat as **your** environment snapshot, not a universal constant).

**Legacy:**

- Older **`jmi_gold` / `jmi_analytics`** / flat Silver layouts are **not** the active story; DDL snapshots live under `infra/aws/athena/archive_non_v2_ddl/` for archaeology. v1 vs v2 strategy is documented in `docs/MIGRATION_V1_V2.md`.

---

## 9. What is intentionally not core right now

- **Glue Crawler as the source of truth:** Not the design. **DDL + partition projection** match the **exact** S3 layout your Python writers emit; crawlers are easy but **imprecise** for this project’s enum partitions and `run_id` handling.
- **Always-on schedule:** EventBridge may be **off** during grading windows to control cost and surprise runs—pipeline code is valid without 24/7 firing.
- **Adzuna on the same schedule as Arbeitnow:** Code supports it; **default Lambda wiring** may still be **Arbeitnow-first**—see `infra/aws/lambda/README.md`.
- **“One click” Terraform for everything:** The repo ships **IAM samples**, **DDL**, **handlers**, and **scripts**; much of **Glue/Athena/QuickSight** is **manually reproducible** from files—that is a **valid** capstone story if you can justify it (§10).
- **Legacy paths:** `gold_legacy/`, `silver_legacy/`, old **`ingest_month=`** partitions—only matter if you still have historical objects; **current writers** use **`posted_month=`** per `paths.py`.

---

## 10. Why this is a valid AWS project even with manual DDL and no crawler

**Project-specific reasons:**

- **Your partitions are not “discover-only”:** Gold DDL uses **`projection.enabled=true`** with **`projection.source.values`** (e.g. `arbeitnow,adzuna_in`) and **`projection.run_id.values`** as an **explicit enum**—that is **Glue metadata as infrastructure**, tightly coupled to pipeline batches. A crawler would not **know** your enum `run_id` set without extra work, and could **mis-infer** types.
- **Views are first-class deliverables:** Much of the “product” is **`jmi_analytics_v2`** SQL—benchmark rows, aligned skill mix, HHI narratives—versioned in git and deployed to Athena. That is **real analytics engineering**, not a toy wrapper around autogenerated tables.
- **Cost and predictability:** You can explain **every** `LOCATION` prefix in DDL against **`paths.py`** output—reviewers see **intentional** design, not accidental discovery.
- **Operational reality:** Many teams use **DDL in CI/CD** or migration scripts; your **browser-console** story is **equivalent** to “Infrastructure as SQL artifacts in git,” which this repo already contains.

---

## 11. How to explain the project

### 30 seconds

“I built a serverless job-market pipeline: APIs → S3 Bronze/Silver/Gold Parquet → Glue and Athena → QuickSight. Bronze keeps raw JSON for audit; Silver dedupes jobs and extracts skills; Gold holds small monthly aggregates by skill, role, location, and company. The dashboard **`dea final 9`** is the v2 QuickSight build on **`jmi_analytics_v2`**. ”

### 1 minute

“Vendors expose noisy JSON job postings. I ingest into **Bronze** as gzipped JSONL with **`run_id`** lineage, then normalize into **Silver Parquet**—one row per job with deterministic **`job_id`** and rule-based skills. **Gold** rolls up to monthly facts partitioned by **`source`**, **`posted_month`**, and **`run_id`**. I registered **external tables** in **`jmi_gold_v2`** and **`jmi_silver_v2`** with **partition projection**, and put presentation logic in **`jmi_analytics_v2`** views so BI can evolve. Athena powers **QuickSight**; my capstone dashboard is **`dea final 9`**, showing EU, India, and comparison slices. Lambda runs the same Python as local, chained ingest→silver→gold.”

### 3 minutes

Expand with: **why medallion** (replay + audit); **two sources** (Arbeitnow EU vs Adzuna India) and **`source=`** partitions; **pointer files** `latest_run_metadata_*` for “latest run” semantics; **comparison views** (aligned months, skill mix, concentration) and why they live in SQL not QuickSight calculated fields; **proof row** `pipeline_run_summary`; **no crawler** because **projection enums** match writer contracts; **trade-off** of manual **`run_id`** list updates when new batches land; **optional** EventBridge off for cost; **Uncertainty:** exact QuickSight wiring for **`dea final 9`** confirmed in console.

---

## 12. Key terms to remember

| Term | Meaning in JMI |
|------|----------------|
| **`source`** | Slug for origin feed (`arbeitnow`, `adzuna_in`)—partitions S3 and Gold tables. |
| **`run_id`** | One id per ingest batch—threads Bronze → Silver → Gold paths and quality JSON. |
| **`posted_month`** | Gold partition key: **calendar month of job posting** (from Silver `posted_at`), **not** batch ingest date. |
| **`jmi_gold_v2`** | Glue DB: **physical** Gold fact tables + metadata pointers over `gold/…` prefixes. |
| **`jmi_silver_v2`** | Glue DB: **Silver merged** (and similar) external tables for row-grain QA / helpers. |
| **`jmi_analytics_v2`** | Glue DB: mostly **views** for KPIs, pareto/clean rolls, geo/sankey helpers, **comparison_***. |
| **Partition projection** | Glue table property so Athena can plan partitions without `MSCK REPAIR` for every new prefix—**requires** predicate patterns and **updated** `run_id` enums when needed. |
| **`pipeline_run_summary`** | Gold table with **PASS/FAIL**-style validation counts—dashboard “proof” sheet material. |
| **`latest_run_metadata_*`** | Small pointer table/Parquet for **which `run_id`** is “current” per source—drives “latest” views. |
| **HHI (skill-tag)** | Concentration metric used in comparison benchmark views—**not** the same as “jobs per skill” row counts; read SQL definitions before explaining. |
| **`dea final 9`** | **Your** published QuickSight dashboard name for the v2 outcome—**dataset bindings** are console-verified. |

---

## 13. Five likely teacher questions

1. **Why three Glue databases instead of one?**  
   **Answer sketch:** **Physical facts** (`jmi_gold_v2`) vs **row-grain Silver** (`jmi_silver_v2`) vs **logical/presentation views** (`jmi_analytics_v2`)—separation keeps DDL churn out of fact tables and mirrors how BI tools pick datasets.

2. **Why not query Silver directly for everything?**  
   **Answer sketch:** Silver is **wide and job-level**; dashboards would **scan too much** and **re-aggregate inconsistently**. Gold fixes **grain** (monthly) and **cost**.

3. **How do you know a dashboard number matches a pipeline run?**  
   **Answer sketch:** **`run_id`** on Gold rows + **`pipeline_run_summary`** + pointer metadata; views filter to **latest** `run_id` via `latest_run_metadata_*` patterns.

4. **Why manual DDL and partition projection instead of a crawler?**  
   **Answer sketch:** **Explicit** alignment with writer paths and **enum** partitions (`source`, `run_id`); avoids schema drift and keeps Athena plans **predictable**.

5. **What is the hardest real-world issue you handled?**  
   **Answer sketch (pick what is true for you):** **Aligned months** across EU/India when coverage differs—drives **intersection** logic in comparison SQL; or **partition projection** maintenance when **`run_id`** updates; or **cost** of Athena/S3 LIST if queries omit filters—tie to **Gold-first** design.

---

## 14. One strong viva-ready summary paragraph

**Job Market Intelligence** ingests public job APIs into an **Amazon S3 medallion lake**—**Bronze** JSONL for audit, **Silver** deduplicated Parquet jobs with rule-based skills, **Gold** monthly aggregates partitioned by **`source`**, **`posted_month`**, and **`run_id`**—then exposes **Glue-cataloged** tables in **`jmi_gold_v2`** and **`jmi_silver_v2`** and layers **presentation SQL** in **`jmi_analytics_v2`** so **Amazon Athena** can serve **Amazon QuickSight** without rescanning raw JSON. The **active v2** path replaces legacy single-layout catalog names; the capstone dashboard **`dea final 9`** is the consolidated BI outcome on this stack, with **lineage and validation** (`pipeline_run_summary`, metadata pointers) grounding trust. **Uncertainty:** exact **QuickSight dataset IDs** and **per-visual** bindings for **`dea final 9`** must be read from the **QuickSight console**, not this repo; whether **EventBridge** auto-ingest and **Adzuna Lambda** scheduling are enabled in **your** account is **environment-specific**; **AWS account/bucket** strings embedded in DDL/scripts should be **verified live** if your deployment differs.

---

## Uncertainty (explicitly not guessed)

- **`dea final 9`:** Which datasets attach to which visuals—**only in QuickSight**; repo gives **candidate** views and naming (`docs/project_study_guide.md`, `QUICKSIGHT_V2_DATASET_STRATEGY.md`).
- **Schedules:** EventBridge **enabled/disabled** and **cron/rate**—confirm in console; JSON under `infra/aws/eventbridge/` is **intent**, not guaranteed live state.
- **Adzuna in Lambda:** Image includes Adzuna code; **default scheduled handler** is Arbeitnow—confirm whether **you** deployed a separate Adzuna function or run India **locally**.
- **Historical S3 keys:** Some old docs/checkpoints mention **`ingest_month=`** paths; **current** code and v2 DDL use **`posted_month=`**—if you still have old objects, call that out as **migration residue** rather than the active contract.

---

# AWS infrastructure (deep study note — v2 / live path)

**Audience:** Personal study and viva prep.  
**Voice:** As if *you* built and operated this: **console** (S3, Glue, IAM, QuickSight, Billing), **Athena query editor** for DDL/views, **Lambda + ECR** for pipeline code, **EventBridge** for time triggers.  
**Scope:** **Current active v2** infra that feeds **`dea final 9`** and the **ingest → Silver → Gold** chain; legacy v1 Glue objects **only** where contrast clarifies.  
**Uncertainty:** Live **IAM** may be broader than `infra/aws/iam/lambda-execution-policy.json`; **EventBridge schedule state**; **QuickSight** wiring; **account/bucket IDs** in scripts vs your environment—see §“Infra uncertainty” at the end.

---

## 1. High-level AWS architecture map (this project)

```text
                    ┌─────────────────────────────────────────────────────────────┐
                    │  Internet: Arbeitnow / Adzuna APIs (HTTPS)                  │
                    └───────────────────────────────┬─────────────────────────────┘
                                                    │
                    ┌───────────────────────────────▼─────────────────────────────┐
                    │  AWS Lambda (container images from ECR)                         │
                    │  jmi-ingest-live → jmi-transform-silver → jmi-transform-gold   │
                    │  (async invokes; Gold may run Athena DDL to refresh Glue props) │
                    └───────────────────────────────┬─────────────────────────────┘
                                                    │
                    ┌───────────────────────────────▼─────────────────────────────┐
                    │  Amazon S3 (single data bucket — medallion + athena-results) │
                    │  bronze/ / silver/ / gold/ + gold/source=*/latest_run_metadata │
                    └───────────────────────────────┬─────────────────────────────┘
                                                    │
                    ┌───────────────────────────────▼─────────────────────────────┐
                    │  AWS Glue Data Catalog (Glue databases + table metadata)   │
                    │  jmi_gold_v2 + jmi_silver_v2 + jmi_analytics_v2             │
                    └───────────────────────────────┬─────────────────────────────┘
                                                    │
                    ┌───────────────────────────────▼─────────────────────────────┐
                    │  Amazon Athena (SQL) — same workgroup as QuickSight DS     │
                    └───────────────────────────────┬─────────────────────────────┘
                                                    │
                    ┌───────────────────────────────▼─────────────────────────────┐
                    │  Amazon QuickSight — dashboard `dea final 9` (SPICE/DQ)     │
                    └─────────────────────────────────────────────────────────────┘

Time-based trigger (optional / account-specific): EventBridge Scheduler → Lambda ingest.

Cross-cutting: IAM roles/policies, CloudWatch Logs per Lambda, Billing/Cost tools (recommended), CloudShell for CLI only (not Docker builds).
```

---

## 2. Full list of AWS services actually used

| Service | Role in JMI |
|--------|-------------|
| **Amazon S3** | System of record for Bronze/Silver/Gold Parquet & JSONL; Athena result prefix; optional `lambda_legacy/` zip archive. |
| **Amazon ECR** | Stores **Docker images** for Lambda (`PackageType: Image`)—not optional for the documented deploy path. |
| **AWS Lambda** | Three functions: ingest → async Silver → async Gold (`infra/aws/lambda/handlers/`). |
| **Amazon EventBridge Scheduler** | Periodic trigger to **`jmi-ingest-live`** (repo JSON: `infra/aws/eventbridge/jmi-ingest-schedule.json`). |
| **AWS Glue Data Catalog** | Databases + **external table** definitions + **views** for Athena; **partition projection** metadata on Gold. |
| **Amazon Athena** | Interactive DDL/DML, **views** in `jmi_analytics_v2`; **Gold Lambda** runs `ALTER TABLE … SET TBLPROPERTIES` to refresh `projection.run_id.values`. |
| **Amazon QuickSight** | BI on Athena datasets; **`dea final 9`** is the **named** dashboard outcome. |
| **AWS IAM** | Execution role for Lambda (S3, invoke chain, logs); separate role for EventBridge to invoke Lambda. |
| **Amazon CloudWatch Logs** | Lambda stdout/stderr (ingest errors, Silver/Gold tracebacks, projection sync). |
| **AWS Billing / Cost Management** | Budget/cap mindset (`docs/cost_guardrails.md`); **Cost Anomaly Detection** as **recommended** guardrail, not app code. |
| **AWS CloudShell** | Browser CLI for `aws` commands (no Docker—see Lambda README). |

**Related (not “runtime” but real):** **GitHub Actions** or **AWS CodeBuild** for **ECR image build/push** when local Docker is unavailable (`.github/workflows/jmi-lambda-ecr-deploy.yml`, `infra/aws/lambda/codebuild/buildspec.yml`).

---

## 3. Per-service deep dive (what / why / alternative / why not)

### Amazon S3

- **What it is:** Object storage; this project uses **one primary bucket** (DDL examples: `jmi-dakshyadav-job-market-intelligence`) for **all** lake prefixes.
- **Exact role:** Holds **Bronze** `*.jsonl.gz`, **Silver** Parquet (including `merged/`), **Gold** fact Parquet under `gold/<table>/source=…/posted_month=…/run_id=…/`, **metadata** pointers under `gold/source=<slug>/latest_run_metadata/`, **`athena-results/`** for Athena query output, optional **`quality/`**, **`health/`**.
- **Repo / config:** `src/jmi/paths.py`, `src/jmi/config.py` (`JMI_DATA_ROOT` / bucket env), DDL `LOCATION 's3://…'` in `infra/aws/athena/ddl_*.sql`, `infra/aws/iam/lambda-execution-policy.json` (bucket scoped).
- **Data / control flow:** **Write path:** Lambda pipelines **PutObject**; **read path:** Athena **GetObject** via table **LOCATION**; **Gold handler** **ListObjectsV2** under `gold/role_demand_monthly/` to discover `run_id=` segments for projection sync (`src/jmi/aws/athena_projection.py`).
- **Why chosen:** Cheapest durable **source of truth** for a student-scale lake; integrates with Athena/Glue without a warehouse.
- **Alternatives:** EFS for files, RDS for tables, DynamoDB for key-value.
- **Why not those:** You need **immutable bulk files** + **columnar analytics** + **partitioned** layout; RDS/EFS would **cost more** and **fight** the Athena model.
- **Pros:** Cheap, durable, native to Athena/Glue. **Cons:** **LIST operations** and **bad query patterns** can surprise you on cost (see project cost notes).

### AWS Lambda (+ Amazon ECR)

- **What it is:** **Serverless** functions; here **container images** from **ECR**, not zip layers, for Linux parity and full `src/` tree.
- **Exact role:** **`jmi-ingest-live`:** runs `ingest_live`, writes Bronze, **async-invokes** Silver. **`jmi-transform-silver`:** runs `transform_silver`, writes Silver, **async-invokes** Gold. **`jmi-transform-gold`:** runs `transform_gold`, writes Gold, then **`sync_gold_run_id_projection_from_s3()`** (lists S3, runs **Athena ALTER TABLE** to update Glue `projection.run_id.values` on fact tables).
- **Repo / config:** `infra/aws/lambda/Dockerfile`, `handlers/*.py`, `deploy_ecr_create_update.sh`, `update_lambdas_from_image_uri.sh`, `.github/workflows/jmi-lambda-ecr-deploy.yml`, `codebuild/buildspec.yml`.
- **Data / control flow:** **Control:** async `lambda:InvokeFunction` between stages (`ingest_handler.py`, `silver_handler.py`). **Data:** S3 read/write for each stage; **Gold** additionally **Athena API** for metadata updates.
- **Why chosen:** **No idle servers**; pay per run; matches **micro-batch**; same Python as local.
- **Alternatives:** ECS Fargate scheduled tasks, EC2 cron, Airflow on EMR.
- **Why not those:** **Ops overhead** and **cost floor** for a capstone; Lambda + S3 is **minimal moving parts**.
- **Pros:** **Scales to zero**, fits `$3` cap mindset. **Cons:** **Cold start**, **15‑min timeout** ceiling, **must** package dependencies cleanly; **async invoke** means **caller does not wait** for Gold—good for latency, but you must **inspect logs** if something fails downstream.

### Amazon EventBridge Scheduler

- **What it is:** **Time-based scheduling** (successor-style API to “run this on a schedule”); repo file targets **Lambda** `jmi-ingest-live`.
- **Exact role:** Triggers **ingest** on a **rate** (repo: `rate(24 hours)`; schedule **Name** still says `jmi-ingest-10min`—historical mismatch worth fixing in console).
- **Repo / config:** `infra/aws/eventbridge/jmi-ingest-schedule.json`, `scheduler-target.json` if present, `docs/project_study_guide.md` (notes schedule may be **disabled** for validation).
- **Data / control flow:** **Control only**—small JSON **Input** payload to Lambda; **no** data payload from S3.
- **Why chosen:** **Native** AWS scheduler → Lambda; no process to keep alive.
- **Alternatives:** **EventBridge Rules** (cron) with `lambda:InvokeFunction`, **external cron** hitting HTTP (not applicable here), **Lambda URL + external ping**.
- **Why not those:** Scheduler is **simple and first-class** for periodic Lambda; external cron adds **secrets and infra** outside AWS.
- **Pros:** Reliable, IAM-auditable. **Cons:** **Misnamed** schedule in repo vs rate; **ENABLED** in JSON **may not** match live—**verify**.

### AWS Glue Data Catalog

- **What it is:** The **metadata catalog** Athena uses (Glue **databases**, **tables**, **views**). *Not* the same as “Glue ETL jobs.”
- **Exact role:** **`jmi_gold_v2`**, **`jmi_silver_v2`**, **`jmi_analytics_v2`**; **external tables** over S3; **partition projection** `TBLPROPERTIES` so Athena can **plan** partitions without crawling every prefix.
- **Repo / config:** `infra/aws/athena/ddl_gold_*.sql`, `ddl_silver_v2_*.sql`, `analytics_v2_*.sql`, `scripts/deploy_athena_v2.py` (patch `jmi_gold` → `jmi_gold_v2`); **Gold Lambda** updates **`projection.run_id.values`** after each Gold run.
- **Data / control flow:** **Metadata** flows **Athena DDL → Glue**; **bytes** still live in **S3**. **Gold Lambda** runs `ALTER TABLE … SET TBLPROPERTIES` via Athena.
- **Why chosen:** **Athena requires** a catalog; Glue is the **default** integrated catalog.
- **Alternatives:** **Hive metastore** on RDS, **Apache Iceberg** only catalog (heavier), **AWS Lake Formation** (adds governance—optional).
- **Why not those:** Glue is **zero extra servers** and **first-class** with Athena.
- **Pros:** **Serverless metadata**. **Cons:** **Manual** `run_id` enum maintenance unless automation is perfect—this project **automates** via Gold Lambda but **depends on** Athena permissions.

### Amazon Athena

- **What it is:** **Serverless** Presto/Trino-style SQL over S3 via Glue catalog.
- **Exact role:** **Dashboard queries** for QuickSight; **ad-hoc** validation; **DDL** for views; **Glue property updates** from **Gold Lambda** (`athena_projection.py`).
- **Repo / config:** `infra/aws/athena/*.sql`, `scripts/deploy_athena_v2.py`, `scripts/athena_smoke_v2.py`, workgroup typically **`primary`** (see projection code).
- **Data / control flow:** **Read:** Parquet from S3 through Glue **LOCATION**. **Write:** `athena-results/` (query results + DDL metadata). **Control:** `ALTER TABLE` changes **Glue** table properties.
- **Why chosen:** **No cluster** to run; pay per query; **same SQL** as in coursework.
- **Alternatives:** **Redshift Spectrum**, **RDS** + ETL, **Spark SQL** on EMR.
- **Why not those:** **Cost and complexity** for MVP; **Gold-first** keeps scans small.
- **Pros:** **Elastic**. **Cons:** **Scans cost money**—must filter on **`source`**, **`posted_month`**, **`run_id`** where possible.

### Amazon QuickSight

- **What it is:** **Managed BI** with **SPICE** (in-memory cache) or **Direct Query** to Athena.
- **Exact role:** **`dea final 9`** — **presentation** of **EU**, **India**, **comparison** slices backed by **`jmi_analytics_v2`** (and sometimes **`jmi_gold_v2`** facts).
- **Repo / config:** `scripts/quicksight_create_datasets_v2.py` (example dataset creation), `docs/dashboard_implementation/QUICKSIGHT_*.md`, `QUICKSIGHT_V2_DATASET_STRATEGY.md`.
- **Data / control flow:** **Athena data source** → **datasets** (views/tables) → **analysis** → **published dashboard**; **refresh** schedule if SPICE.
- **Why chosen:** **Teacher-facing** visuals; **native** Athena connector; **no** custom frontend required for viva.
- **Alternatives:** **Streamlit** (repo `dashboard/app.py` — **dev/local**), **Grafana**, **Superset**, **hosted Metabase**.
- **Why not those:** QuickSight is **AWS-native** and matches **capstone** expectations; Streamlit is **not** the named **`dea final 9`** outcome unless you frame it explicitly.
- **Pros:** **Sharing**, **SPICE** for demos. **Cons:** **Authoring** is in-console; **no** full dashboard JSON in git for **`dea final 9`**—**verify** bindings.

### AWS IAM

- **What it is:** **Identity and access** for AWS API calls.
- **Exact role:** **Lambda execution role** (`jmi-lambda-exec-role` in deploy script) — **S3** read/write/list, **Lambda invoke** for chain, **CloudWatch Logs**; **EventBridge role** to **invoke** ingest Lambda (`jmi-eventbridge-invoke-lambda-role` in schedule JSON).
- **Repo / config:** `infra/aws/iam/lambda-execution-policy.json`, `lambda-trust-policy.json`, `infra/aws/eventbridge/jmi-ingest-schedule.json` (RoleArn).
- **Data / control flow:** **Control plane only**; **no** IAM “data path” except **authorization** on every S3/Athena/Lambda call.
- **Why chosen:** **Least privilege** per function; **no** long-lived keys in repo (secrets in env/CI).
- **Alternatives:** **Wildcard admin** (bad), **resource-based policies** only (awkward for Lambda).
- **Why not those:** **Examiner** expects **IAM** story.
- **Pros:** Auditable. **Cons:** **Must** match real needs—**Gold Lambda needs Athena `StartQueryExecution`**; **checked-in** policy sample may be **incomplete** (see Uncertainty).

### Amazon CloudWatch Logs

- **What it is:** **Centralized log** storage for Lambda and other services.
- **Exact role:** **Debug** ingest failures, Silver exceptions, **Gold projection** failures; **traceback** in `gold_handler.py` on projection errors.
- **Repo / config:** Implicit via Lambda; **permissions** in `lambda-execution-policy.json` (`logs:CreateLogGroup`, etc.).
- **Data / control flow:** **Lambda** → **LogStreams** (text); **not** queried by QuickSight for BI.
- **Why chosen:** **Default** Lambda integration; **no** extra log stack.
- **Alternatives:** **OpenSearch**, **Datadog**, **ELK**.
- **Why not those:** **Cost and complexity** for MVP.
- **Pros:** **Free tier** friendly at small volume. **Cons:** **Logs are not metrics**—you still need **alarms** if you want paging (optional).

### AWS Billing / Cost Anomaly Detection

- **What it is:** **Cost Explorer** + **Budgets** + **Anomaly Detection** on **AWS billing** data.
- **Exact role:** **Guardrail** against surprise **Athena** or **S3 request** spikes (see `docs/cost_guardrails.md`, `docs/project_study_guide.md` cost section).
- **Repo / config:** `docs/cost_guardrails.md`, `docs/runbook.md` (budget alarms), **not** automated in code.
- **Data / control flow:** **Metadata** flows **Billing → console/alerts**; **no** pipeline data.
- **Why chosen:** Student **$3 cap** and **lesson** on serverless **gotchas** (LIST, scan size).
- **Alternatives:** **Ignore costs** (risky), **third-party FinOps**.
- **Why not those:** **Native** and **free tier** for basic budgets.
- **Pros:** **Early warning**. **Cons:** **Does not** stop bad queries—**design** still matters.

### AWS CloudShell

- **What it is:** **Browser** shell with **AWS CLI** preinstalled.
- **Exact role:** Run **`aws glue get-table`**, **`aws athena start-query-execution`**, **`aws s3 ls`**, **`aws quicksight list-data-sets`** without **local** AWS CLI setup. **Not** for **Docker** builds (Lambda README).
- **Repo / config:** Mentioned in `infra/aws/lambda/README.md`, `codebuild/buildspec.yml` header.
- **Data / control flow:** **CLI only**; **no** automatic link to pipeline.
- **Why chosen:** **Zero install** on laptop.
- **Alternatives:** **Local terminal**, **Cloud9**.
- **Why not those:** **Fastest** for quick checks in exam prep.
- **Pros:** **Convenient**. **Cons:** **No Docker**—use **GitHub Actions** or **CodeBuild** for ECR images.

---

## 4. Services to cover deeply (summary table)

| Service | One-line “why here” |
|--------|----------------------|
| **S3** | Durable lake + Athena results; projection sync **lists** Gold keys. |
| **Lambda** | **Only** compute for scheduled/micro-batch transforms. |
| **EventBridge Scheduler** | **Time trigger** for ingest (if enabled). |
| **Glue Data Catalog** | **Schema + projection** for Athena; **views** for BI. |
| **Athena** | **SQL** for BI + **DDL** to maintain Glue after Gold. |
| **QuickSight** | **`dea final 9`** consumer of Athena datasets. |
| **IAM** | **Least privilege** for S3/Lambda/logs (+ **Athena** in live—verify). |
| **CloudWatch Logs** | **Ops truth** for pipeline failures. |
| **Billing / Anomaly** | **Capstone** cost discipline. |
| **CloudShell** | **Console** CLI for ops without local Docker. |

---

## 5. Full interaction map (end-to-end)

1. **Source API → Lambda ingest:** `jmi-ingest-live` **`ingest_handler`** calls **`ingest_live`** (Arbeitnow) → HTTP fetch → **Bronze** path resolution.
2. **Lambda ingest → Bronze in S3:** **`PutObject`** to `bronze/source=arbeitnow/ingest_date=…/run_id=…/raw.jsonl.gz` (+ manifest/health as implemented).
3. **Lambda ingest → Silver trigger:** **`lambda_client.invoke`**, **InvocationType: Event**, payload `{ bronze_file, run_id }` → **`jmi-transform-silver`**.
4. **Lambda Silver → Silver in S3:** **`silver_run`** writes Parquet under `silver/jobs/source=…/…` and merged **`latest.parquet`** where applicable.
5. **Lambda Silver → Gold trigger:** **`async invoke`** Silver → Gold with `{ silver_file, merged_silver_file, run_id }` (`silver_handler.py`).
6. **Lambda Gold → Gold in S3:** **`gold_run`** writes **`gold/<table>/source=…/posted_month=…/run_id=…/`** + pointers under `gold/source=<slug>/latest_run_metadata/`.
7. **Gold in S3 → Glue metadata / Athena tables:** **Initial** DDL from **`deploy_athena_v2.py`** + manual SQL; **each Gold run** **`sync_gold_run_id_projection_from_s3`** lists **`run_id`** prefixes and runs **`ALTER TABLE jmi_gold_v2.<fact> SET TBLPROPERTIES ('projection.run_id.values'='…')`** via **Athena** so **new** Parquet files are **visible** to projection.
8. **Athena views → QuickSight datasets:** **Data source** (Athena) → datasets bound to **`jmi_analytics_v2.<view>`** (and optionally **`jmi_gold_v2.<table>`**); naming in `QUICKSIGHT_V1_V2_NAMING.md`.
9. **QuickSight datasets → visuals in `dea final 9`:** **Analysis** → **published dashboard**; **exact** mapping **console-only** (Uncertainty).

---

## 6. Services intentionally not used as core path

### Glue Crawler

- **Would have done:** **Infer** schema and **discover** partitions; **register** tables automatically.
- **Why others use it:** **Fast** to bootstrap unknown data.
- **Why this project did not:** **Partition projection** + **enum** `run_id`/`source` values **must** match **writers**; **crawler** can **mis-infer** types or **partition** layout; **DDL in git** is the **contract**.
- **Good decision?** **Yes** for this repo’s **controlled** layout; **manual** `run_id` list would be painful **without** Gold Lambda’s **sync**—you automated the worst part.

### Glue ETL Jobs

- **Would have done:** **Serverless Spark** transforms on Glue.
- **Why others use it:** **Big** joins, **heavy** cleansing at scale.
- **Why this project did not:** **Python** transforms are **already** in **`src/jmi/pipelines/`**; **Lambda** runs the same code; **no** Spark team or **10 TB** job.
- **Good decision?** **Yes** for MVP **cost** and **simplicity**.

### Step Functions

- **Would have done:** **Orchestrate** ingest → silver → gold with **retries**, **state**, **visualization**.
- **Why others use it:** **Complex** multi-step **SLAs** and **error handling**.
- **Why this project did not:** **Three** Lambdas with **async invoke** is **enough**; **state** is in **S3 + run_id**; **no** human approval steps.
- **Good decision?** **Yes** at this scale; **revisit** if you add **many** branches or **compensation** logic.

### EC2

- **Would have done:** **Always-on** servers for cron + Python.
- **Why others use it:** **Long** jobs, **custom** OS, **legacy** apps.
- **Why this project did not:** **Violates** “**no always-on compute**” (`cost_guardrails.md`); **Lambda** covers **batch** duration here.
- **Good decision?** **Yes** for **cost cap**.

### Redshift / warehouse-first

- **Would have done:** **Load** Gold into **columnar warehouse**, **SQL** there, **QuickSight** on Redshift.
- **Why others use it:** **Sub-second** BI at **large** concurrency; **joins** across **many** curated tables.
- **Why this project did not:** **Athena + Parquet + Gold** is **small enough**; **no** **cluster** budget or **admin** overhead.
- **Good decision?** **Yes** for MVP; **revisit** if **concurrent** users and **complex** joins explode.

### EMR

- **Would have done:** **Spark** cluster for **big** Silver/Gold.
- **Why others use it:** **Massive** scale, **complex** graph processing.
- **Why this project did not:** **Not** relevant to **MB–low GB** job data; **Lambda + Python** suffices.
- **Good decision?** **Yes**.

---

## 7. Why this AWS architecture is defensible in viva

- **End-to-end serverless data path:** **S3 + Glue + Athena + QuickSight** is a **standard** analytics lake pattern; **you** can **point to files** in repo (`paths.py`, DDL) that **match** S3 keys.
- **Explicit contracts over magic:** **Manual DDL + projection** + **Gold Lambda sync** shows **you** understood **Glue metadata**, not just “click crawler.”
- **Same code local and cloud:** **Lambda** runs **`src/jmi`** — **reproducible** story.
- **Cost awareness:** **Gold-first**, **partition filters**, **student cap** in docs—**adult** engineering.

---

## 8. What to say in front of the teacher (infra)

“I used **S3** as the lake for Bronze, Silver, and Gold **Parquet**; **Lambda** in a **Docker image from ECR** runs the same **Python** as my laptop. **Ingest** triggers **Silver** and **Gold** asynchronously. After **Gold** writes new **`run_id`** folders, **Gold Lambda** runs **Athena** `ALTER TABLE` to refresh **Glue partition projection** so **Athena** can see new runs. **Glue Data Catalog** holds **`jmi_gold_v2`**, **`jmi_silver_v2`**, and **`jmi_analytics_v2`** views. **QuickSight** connects to **Athena**—that’s **`dea final 9`**. I **did not** use **Glue crawlers** or **EMR** because the **pipeline** owns the **schema** and **partition layout**. **EventBridge** can trigger **daily** ingest; **I** may have **disabled** it during testing. **IAM** and **CloudWatch** cover **permissions** and **logs**; **cost guardrails** are in **Billing** tools.”

---

## 9. Key terms to remember (infra)

| Term | Meaning |
|------|----------|
| **PackageType Image** | Lambda from **ECR** container image (this project’s deploy path). |
| **Async invoke** | Fire-and-forget **Lambda** call—ingest returns before **Gold** finishes. |
| **Partition projection** | Glue table property so Athena **does not** need **MSCK** for every new prefix—**requires** predicate patterns and **accurate** `run_id` enums. |
| **`sync_gold_run_id_projection_from_s3`** | Project-specific **automation**: **S3 list** → **Athena ALTER** → **Glue** updated. |
| **Workgroup** | Athena **workgroup** (e.g. `primary`) for `StartQueryExecution` + **result** location. |
| **SPICE vs Direct Query** | QuickSight **cache** vs **live** Athena—**trade-off** of freshness vs performance. |

---

## 10. Five likely teacher questions (infra)

1. **How does Lambda know the new Gold files are queryable?**  
   **Answer sketch:** **Partition projection** lists **`run_id`**; **Gold Lambda** **lists S3** and **updates** `projection.run_id.values` via **Athena DDL** on **`jmi_gold_v2.*`** fact tables.

2. **Why three Lambdas instead of one?**  
   **Answer sketch:** **Separation** of stages, **independent** timeouts/retries, **async** chaining—**smaller** failure blast radius; **same** image, different **handler** command.

3. **Why Athena and not Redshift?**  
   **Answer sketch:** **No cluster** cost; **Gold** tables are **small**; **Parquet** on **S3** is the **source of truth**.

4. **What if EventBridge fires during a bad deploy?**  
   **Answer sketch:** **Disable** schedule in console; **rollback** image to **known** tag; **idempotent** runs tied to **`run_id`** reduce **silent** corruption.

5. **Where is IAM least privilege demonstrated?**  
   **Answer sketch:** Lambda role **scoped** to **one bucket** prefix and **specific** downstream function ARNs; **EventBridge** uses **separate** invocation role—**verify** **Athena** on **Gold** in **live** policy.

---

## 11. One viva-ready summary paragraph (infra)

**This** project’s AWS side is **deliberately serverless:** **S3** holds the **medallion** lake; **ECR-backed Lambdas** run **ingest → Silver → Gold** with **async invokes**; **Glue Data Catalog** registers **external** Gold/Silver tables and **analytics views** with **partition projection**; **Gold Lambda** **lists** new **`run_id`** prefixes in S3 and **runs Athena** `ALTER TABLE` to keep **Glue** `projection.run_id.values` **in sync**—so Athena can **query** fresh **`dea final 9`** datasets without a **crawler**. **QuickSight** reads **Athena**; **IAM** and **CloudWatch Logs** cover **access** and **debugging**; **cost** is controlled by **Gold-first** queries and **budget** awareness. **Glue crawlers**, **EMR**, and **Redshift** were **out of scope** for this **MVP** because **schema** and **volume** are **owned** by the **Python** pipeline, not **discovered** at petabyte scale.

---

# S3 and storage layout (deep study note — active v2)

**Audience:** Personal study and viva prep.  
**Voice:** As if *you* navigated the **S3 console**—checking prefixes, verifying `source=` / `posted_month=` / `run_id=` segments, and correlating keys with **Athena** DDL and **`dea final 9`** datasets.  
**Source of truth:** `src/jmi/paths.py`, `src/jmi/config.py` (`DataPath`, roots), pipeline modules, `infra/aws/athena/ddl_*.sql` **LOCATION** clauses.  
**Scope:** **Active** modular layout (`source=` everywhere, Gold **`posted_month=`**). Legacy paths appear only for **contrast**.  
**Uncertainty:** Exact **bucket name** in your account; whether **optional** `derived/`, `gold/comparison_*`, or **slice** env vars are present in **your** bucket.

---

## 1. Why S3 is central in this project

S3 is not “where backups go”—it **is** the **system of record** for JMI’s **medallion lake**. Every stage **writes objects** under **predictable prefixes** so that:

- **Lambda** (ingest/silver/gold) can **PutObject** without a database connection pool.
- **Athena** can treat those prefixes as **table partitions** via **Glue** **LOCATION** + **partition projection**.
- **You** can **list** a prefix in the console and **see** exactly which **batch** (`run_id`) produced which **month** (`posted_month`) of facts—**without** a separate lineage database.

If S3 layout is sloppy, **Athena costs explode**, **QuickSight** shows **wrong months**, and **debugging** (“which file produced this row?”) becomes guesswork. This project **invests** in path design for that reason.

---

## 2. Why object storage was used instead of a database-first design

**Project-specific drivers:**

- **Raw API payloads** are **large, semi-structured JSON**—Bronze is **JSONL.gz** **lines**, not neat rows. **Object storage** is the natural **append-only** capture format.
- **Silver/Gold** are **Parquet files** per batch/month—**columnar** analytics on **S3** matches **Athena’s** execution model (**scan columns**, **partition prune**).
- **Cost guardrails** (`docs/cost_guardrails.md`): **no always-on** RDS/Redshift cluster for MVP; **pay for storage + queries**, not idle OLTP.
- **Same code path locally:** `JMI_DATA_ROOT` can be `data/` or `s3://bucket/`—**DataPath** abstracts **files**; a DB-first design would **fork** the pipeline.

**Trade-off you should own in viva:** A warehouse gives **transactions** and **indexes**; this project **chooses** **immutable files + run_id lineage** for **auditability** and **lake** economics. **Gold** tables are **small**; **OLTP** is the wrong tool.

---

## 3. Full current active prefix map

Assume **`<bucket>`** = your data bucket (DDL examples use `jmi-dakshyadav-job-market-intelligence`—**verify** in S3 console).

| Prefix | Role |
|--------|------|
| **`bronze/`** | Raw **gzip JSONL** per source + ingest day + batch (`raw.jsonl.gz`, manifests as implemented). |
| **`silver/`** | **Job-level Parquet** under `silver/jobs/…`; **`merged/latest.parquet`** for downstream Gold. |
| **`gold/`** | **Fact tables** `gold/<table>/source=<slug>/posted_month=…/run_id=…/` + **per-source** metadata `gold/source=<slug>/latest_run_metadata/`. |
| **`quality/`** | **JSON** quality reports: `silver_quality_<ingest_date>_<run_id>.json`, `gold_quality_<run_id>.json` (from transforms). |
| **`health/`** | Small **JSON pointers** like `latest_ingest.json`, `latest_ingest_adzuna_in.json`—“last successful ingest” hints for ops/UI. |
| **`state/`** | **Incremental connector state:** `state/source=<slug>/connector_state.json` (watermarks, strategy—see `source_state.py`). Optional **`slice=`** segment for isolated Arbeitnow runs. |
| **`athena-results/`** | **Athena** query output (DDL, ad-hoc SQL, **`ALTER TABLE`** from Gold Lambda). Convention: same bucket, **not** part of the medallion. |
| **`derived/`** (optional) | **Comparison / benchmark** outputs **not** mixed into `gold/source=*` native facts—see `paths.py` header. |
| **`lambda_legacy/`** (optional) | **Zip** audit copy of Lambda package—**not** live pipeline data (`infra/aws/lambda/README.md`). |

**Not** under “active v2 story” but may exist: **`silver_legacy/`**, **`gold_legacy/`**, old **`gold/comparison_*`** keys—see §10.

---

## 4. Exact active path patterns and what each component means

### `source=<slug>`

- **Meaning:** **Which feed** produced the object (`arbeitnow`, `adzuna_in`, …).
- **Where:** **Bronze** (after optional slice logic), **Silver** `jobs/`, **Gold** fact paths, **`gold/source=<slug>/latest_run_metadata/`**, **`state/source=<slug>/`**.
- **Glue:** Often a **partition key** with **`projection.source.values`** (e.g. `arbeitnow,adzuna_in` in DDL).

### `ingest_date=<YYYY-MM-DD>`

- **Meaning:** **UTC calendar date** attached to the **Bronze/Silver batch folder**—“this run landed on this day.”
- **Where:** **Bronze** and **Silver batch** paths (`silver/jobs/source=…/ingest_date=…/run_id=…`).
- **Not the same as:** **`posted_month`** (business time of the job ad).

### `posted_month=<YYYY-MM>`

- **Meaning:** **Calendar month of the job posting** derived from Silver **`posted_at`** (via Gold time-axis logic)—**analytics grain** for monthly facts.
- **Where:** **Gold fact** paths only: `gold/<table>/source=…/posted_month=…/run_id=…/part-00001.parquet`.
- **Glue DDL:** Partition projection uses **`projection.posted_month.*`** (range `2018-01`–`2035-12` in repo DDL).

### `run_id=<id>`

- **Meaning:** **One pipeline batch id** (timestamp prefix + short uuid—`new_run_id()` in `config.py`), shared from Bronze through Gold for **that** transform chain.
- **Where:** Bronze, Silver batch, **every** Gold fact partition folder; also **quality** filenames and **lineage** columns in Parquet.
- **Glue:** **`projection.run_id.values`** must list **every** `run_id` prefix present in S3 (this project **syncs** from S3 in **Gold Lambda**).

### Optional: `slice=<tag>` (Arbeitnow only, when `JMI_ARBEITNOW_SLICE` is set)

- **Meaning:** **Isolated experiment** path for Arbeitnow—**parallel** Bronze/Silver/Gold without overwriting “main” prefixes. **Not** required for standard v2.

---

## 5. Why source-prefixed layout matters

- **Multi-source without mixing:** EU and India **must not** share ambiguous keys; **`source=`** in the **path** makes **S3 listing** and **IAM prefix policies** **unambiguous**.
- **Athena partition pruning:** Filters like **`source = 'arbeitnow'`** match **Hive-style** paths; **Glue projection** can **enumerate** allowed sources.
- **Operational clarity:** In the console, you **expand** `gold/skill_demand_monthly/` and **see** `source=arbeitnow/` vs `source=adzuna_in/` **side by side**—no “guess the column” for which feed a file came from.
- **Contrast with legacy:** Old Gold sometimes kept **`source` only inside Parquet** while paths mixed—**harder** to **physically** delete or **archive** one vendor.

---

## 6. Why posted_month matters

- **Business question:** “What did the **market** look like in **March**?” uses **when the job was posted**, not when your **Lambda** happened to run.
- **Stable charts:** **Monthly** KPIs and **comparison** views align on **`posted_month`**; **ingest_date** would **smear** postings into the wrong **reporting month** if used as the Gold partition key.
- **Joins across tables:** Skill, role, location, and company facts **share** **`posted_month`** + **`run_id`** + **`source`**—**consistent** drill paths for **`dea final 9`**.

**Doc drift note:** `docs/STORAGE_LAYOUT_MULTISOURCE.md` still mentions **`ingest_month=`** in one summary table—**current writers** use **`posted_month=`** per `paths.py` and `ddl_gold_*.sql`. Treat **`posted_month`** as **authoritative** for viva.

---

## 7. Why run_id matters

- **Batch identity:** Ties **Bronze file → Silver file → Gold partitions** for **one** execution—if a teacher asks “prove this number,” you trace **`run_id`**.
- **Athena visibility:** **Partition projection** lists **`run_id`** explicitly; **new** batches need **new** segments and **Glue** enum updates (or **Gold Lambda** sync).
- **Idempotence story:** Re-running a pipeline produces a **new** `run_id` **folder**; you **don’t** silently overwrite history—**audit** stays possible.

---

## 8. Why ingest_date matters

- **Operations:** Answers “**when** did we **land** this batch?”—useful for **incremental** debugging and **correlating** with **CloudWatch** Lambda timestamps.
- **Silver/Bronze partitioning:** Co-locates **all** files from **one** **calendar** ingest day before you **merge** into `merged/latest.parquet`.
- **Different from posted_month:** A job **posted in March** might be **ingested in April**; **`ingest_date`** tracks **your** pipeline clock; **`posted_month`** tracks **labor market** time.

---

## 9. Concrete example paths from this project

Use **`s3://<bucket>/`** prefix; replace **`<bucket>`** and ids with **your** objects.

**Bronze (Arbeitnow):**
`s3://<bucket>/bronze/source=arbeitnow/ingest_date=2026-04-12/run_id=20260412T102534Z-ca1b73ff/raw.jsonl.gz`

**Silver batch + merged:**
`s3://<bucket>/silver/jobs/source=arbeitnow/ingest_date=2026-04-12/run_id=20260412T102534Z-ca1b73ff/part-00001.parquet`  
`s3://<bucket>/silver/jobs/source=arbeitnow/merged/latest.parquet`

**Gold fact:**
`s3://<bucket>/gold/role_demand_monthly/source=arbeitnow/posted_month=2026-03/run_id=20260412T102534Z-ca1b73ff/part-00001.parquet`

**Latest-run pointer (per source):**
`s3://<bucket>/gold/source=arbeitnow/latest_run_metadata/part-00001.parquet`

**Quality / health / state:**
`s3://<bucket>/quality/silver_quality_2026-04-12_20260412T102534Z-ca1b73ff.json`  
`s3://<bucket>/health/latest_ingest.json`  
`s3://<bucket>/state/source=arbeitnow/connector_state.json`

**Athena scratch:**
`s3://<bucket>/athena-results/<query-id>.csv` (exact suffix varies)

---

## 10. Which layouts are active vs legacy

| Active (v2 / current writers) | Legacy / archive |
|-----------------------------|------------------|
| `bronze/source=<slug>/ingest_date=…/run_id=…` | Old keys without **`source=`** (if any remain from early experiments) |
| `silver/jobs/source=<slug>/ingest_date=…/run_id=…` | **`silver/jobs/ingest_date=…`** flat under `jobs/` (pre–source-prefix Arbeitnow) → **`silver_legacy/`** when archived (`paths.py` comments) |
| `gold/<table>/source=<slug>/posted_month=…/run_id=…` | **`gold_legacy/`** with **`ingest_month=`** partitions only—**not** written by current `gold_fact_partition` |
| `gold/source=<slug>/latest_run_metadata/` | Top-level **`gold/latest_run_metadata/`** **without** `source=`—**refused** by `gold_latest_run_metadata_file()` |
| **`jmi_analytics_v2`** views over current paths | Old **`jmi_gold` / `jmi_analytics`** DDL archived under `infra/aws/athena/archive_non_v2_ddl/` |

**Optional orphans:** `gold/comparison_strict_common_month/`, `gold/comparison_yearly/`, etc.—**not** written by core pipeline today; **comparison** logic preferred via **`jmi_analytics_v2.comparison_*`** (`paths.py` header).

---

## 11. Why old/incorrect layouts caused confusion

- **Mixed Gold without `source=` in path:** Hard to **prune** one vendor; **Athena** **LOCATION** had to **scan** everything; **QuickSight** filters relied on **columns** only—**easier** to make mistakes.
- **`ingest_month` vs `posted_month`:** If **charts** used **ingest** time, **March postings** ingested in **April** would **misplace** demand—**teachers** would challenge your **definitions** (`METRIC_DEFINITIONS.md` vs actual partition key).
- **Flat Silver:** **Arbeitnow** and **Adzuna** **looked** like different pipelines; **merged** logic had to **special-case** discovery paths (`transform_gold` / Silver history union).
- **Doc/table drift:** A **STORAGE_LAYOUT** row still saying **`ingest_month=`** for Gold **conflicts** with **`paths.py`**—**fix docs** or **cite code** in viva to show **you** know **`posted_month`** wins.

---

## 12. How S3 path design affects Athena and QuickSight

- **Glue LOCATION + projection:** Table **`LOCATION`** points at `s3://…/gold/<table>/` **without** listing every subfolder; **`source`**, **`posted_month`**, **`run_id`** must **appear in SQL predicates** as **partition keys** for **best** plans (see DDL comments in `ddl_gold_*.sql`).
- **QuickSight:** Datasets **bind** to **`jmi_gold_v2`** tables or **`jmi_analytics_v2`** views—those objects **assume** keys match **DDL**. If you **write** Gold to a **new** `run_id=` but **forget** **Glue** projection update, **Athena returns empty** → **`dea final 9`** **breaks** until **sync** runs.
- **SPICE refresh:** **Snapshot** reflects **whatever** keys existed at refresh time—**path discipline** prevents **accidentally** pulling **wrong** months into **cache**.

---

## 13. How S3 path design helps debugging and lineage

- **Console walk:** Open **`gold/role_demand_monthly/source=arbeitnow/posted_month=2026-03/`**—**multiple** `run_id=` folders show **reruns** or **overlapping** batches for the **same** business month.
- **Quality JSON:** **`quality/gold_quality_<run_id>.json`** ties **validation** to the **same** id as S3 partitions.
- **Pointer file:** **`gold/source=arbeitnow/latest_run_metadata/part-00001.parquet`** gives **one row** “which **`run_id`** is current for EU”—**views** use it for **latest** semantics.
- **State file:** **`state/source=…/connector_state.json`** explains **incremental** behavior without reading **Bronze**.

---

## 14. What to say while showing S3 in viva

“Here’s our **bucket**: **Bronze** is **raw JSONL** by **`source`**, **`ingest_date`**, and **`run_id`**. **Silver** is **Parquet** jobs under **`silver/jobs/source=…`**, with **`merged/latest`** for Gold. **Gold** facts live under **`gold/<table>/source=…/posted_month=…/run_id=…`**—**`posted_month`** is **when the job was posted**, **`run_id`** is **which batch** built the file. **Athena** **external tables** point at these prefixes with **partition projection**. **`quality/`** and **`state/`** are **JSON** for **QA** and **incremental** watermarks. If **`run_id`** folders stop appearing in **Glue** enums, **Athena** won’t see them—I **sync** projection after **Gold**.”

---

## 15. Key terms to remember (S3)

| Term | Meaning in JMI |
|------|----------------|
| **Hive-style keys** | `key=value/` segments in S3 paths → **Glue** partition columns. |
| **`merged/latest.parquet`** | Rolling **Silver** snapshot for **Gold** input—**not** a partition; **single file** pointer. |
| **`gold_fact_partition()`** | Code helper building **canonical** Gold paths (`paths.py`). |
| **`silver_legacy` / `gold_legacy`** | **Archive** layouts—**not** current writers. |
| **Athena results prefix** | **`athena-results/`**—**not** merged into **medallion** metrics. |

---

## 16. Five likely teacher questions (S3)

1. **Why is `posted_month` on the path instead of only in Parquet?**  
   **Answer sketch:** **Partition pruning** and **Glue projection**—**Athena** **skips** irrelevant months **cheaply**; **consistent** with **metric** definitions.

2. **Why both `ingest_date` and `posted_month`?**  
   **Answer sketch:** **Ops** vs **business** time—**ingest** for **batch** folders; **posted** for **labor market** **monthly** facts.

3. **What breaks if `run_id` is missing from Glue projection?**  
   **Answer sketch:** **Athena** **returns no rows** for that batch—**dashboard** **empty**; **Gold Lambda** **sync** fixes **enums** from **S3 LIST**.

4. **Why not one flat folder per layer?**  
   **Answer sketch:** **No pruning**, **higher** scan **cost**, **ambiguous** **multi-source** **mixing**.

5. **Where is lineage if someone doubts a chart?**  
   **Answer sketch:** **`run_id`** in **path** and **Parquet**; **Bronze path** in **Silver** columns; **`pipeline_run_summary`** **Gold** table; **`quality/`** JSON.

---

## 17. One viva-ready summary paragraph (S3)

**Amazon S3** is JMI’s **authoritative lake**: **Bronze** stores **immutable** **JSONL.gz** under **`source=`** / **`ingest_date=`** / **`run_id=`**; **Silver** stores **deduped** **Parquet** jobs under **`silver/jobs/source=…`** with **`merged/latest.parquet`** for downstream reads; **Gold** writes **small** **monthly** facts under **`gold/<table>/source=…/posted_month=…/run_id=…`** plus **per-source** **`gold/source=<slug>/latest_run_metadata/`** pointers—**`posted_month`** is **business** time, **`run_id`** is **batch** identity. **`quality/`**, **`health/`**, and **`state/`** hold **JSON** for **validation**, **ops hints**, and **incremental** watermarks; **`athena-results/`** holds **query** spill—**separate** from **medallion** data. This **layout** **enables** **Glue** **partition projection**, **cheap** **Athena** **filters**, and **traceable** **`dea final 9`** numbers—**legacy** flat or **`ingest_month=`** Gold paths were **abandoned** because they **mixed sources** or **confused** **ingest** vs **posting** time.

---

# Bronze layer (deep study note — active pipeline)

**Audience:** Personal study and viva prep.  
**Source of truth in code:** `src/jmi/pipelines/ingest_live.py`, `src/jmi/pipelines/ingest_adzuna.py`, `src/jmi/connectors/arbeitnow.py` (`to_bronze_record`), `src/jmi/connectors/adzuna.py` (`to_bronze_record`), `src/jmi/pipelines/bronze_incremental.py`, `src/jmi/paths.py` (`bronze_raw_gz`).  
**Uncertainty:** Exact **row counts** per run and **which** incremental strategy is enabled in **your** deployed **Lambda** env (env vars).

---

## 1. What Bronze means generally in data engineering

In a **medallion** architecture, **Bronze** is the **landing zone** for **raw** or **minimally wrapped** data from **upstream systems**. The usual promise: **append-friendly**, **lossless at the boundary you care about**, and **cheap to reprocess** when **Silver** rules change. It is **not** where business KPIs live—it is **evidence** that “the source said X at ingest time.”

---

## 2. What Bronze means in this project specifically

**JMI Bronze** is a **gzip-compressed JSONL** file **`raw.jsonl.gz`** under a **Hive-style path** per **`source`**, **`ingest_date`**, and **`run_id`**. Each **line** is one **Bronze record**: **small fixed metadata** (source, deterministic **`job_id`**, ingest timestamps) plus **`raw_payload`** = the **full vendor JSON** object as returned by the API (Arbeitnow or Adzuna). **No** deduplication, **no** skill extraction, **no** title normalization—those are **Silver**. Bronze is the **contractual snapshot** that lets you **replay** Silver/Gold and **prove** what the API returned.

---

## 3. What enters Bronze in this project

- **Arbeitnow (EU):** `ingest_live.run()` calls **`fetch_all_jobs`** (paginated HTTP to `arbeitnow.com` API), optionally filtered by **incremental** logic (`select_jobs_for_bronze` + connector state). Each returned **`raw`** dict becomes **`to_bronze_record(raw)`** (`arbeitnow.py`).
- **Adzuna (India):** `ingest_adzuna.run()` calls **`adzuna.fetch_all_jobs_india()`**, then the same pattern: **`adzuna.to_bronze_record(job)`** per row (`adzuna.py`).

**What does *not* enter Bronze:** Pre-aggregated KPIs, cleaned Parquet, or merged “latest” tables—those are **downstream**.

---

## 4. What exactly is stored in Bronze

**Per line (conceptually):**

| Field | Role |
|-------|------|
| **`source`** | Slug (`arbeitnow`, `adzuna_in`). |
| **`schema_version`** | e.g. `v1` — version of the **envelope**, not the vendor. |
| **`job_id`** | **Deterministic** hash from stable vendor fields (`build_stable_job_id` / Adzuna equivalent). |
| **`job_id_strategy`** | Audit: `slug`, `url`, `fallback_…`, etc. |
| **`source_slug` / `source_url` / `source_job_id`** | Source-specific identifiers (Adzuna adds **`source_job_id`**). |
| **`ingested_at`** | UTC ISO timestamp when **this** Bronze line was built. |
| **`run_id`** | **Batch** id—added **after** `to_bronze_record` in **`ingest_*`** loops (`ingest_live.py` L49–51, `ingest_adzuna.py` L45–47). |
| **`bronze_ingest_date`** | UTC **calendar date** string for the batch folder. |
| **`batch_created_at`** | UTC ISO timestamp for the **whole** batch. |
| **`raw_payload`** | **Complete** vendor JSON **dict**—untouched structure for replay. |

**Files alongside the gzip:**

- **`manifest.json`** in the **same folder** as `raw.jsonl.gz` (batch-level summary: counts, paths, incremental diagnostics, fetch metadata).
- **`health/*.json`** at **`health_root`** (project-wide “latest ingest” pointer—not inside the bronze prefix, but **about** the latest Bronze run).

---

## 5. Bronze envelope concept in this project

The **envelope** is the **thin wrapper** around **`raw_payload`**: everything except **`raw_payload`** is **your** metadata so rows are **self-describing** without losing vendor fidelity. **`schema_version`** on the envelope lets you **evolve** wrapper fields without pretending the **vendor** schema is stable. **`raw_payload`** stays a **black box** to Bronze—Silver opens it.

---

## 6. Why raw payload is preserved

- **Vendor changes:** APIs add/rename fields; if you only stored mapped columns, **older** runs would be **unrecoverable**.
- **Debugging:** When Silver drops a row or mis-parses a location, you **open Bronze** and **see the exact JSON** the connector saw.
- **Replay:** Improving **`extract_silver_skills`** or title rules does **not** require **re-calling** the API if you still have Bronze (subject to retention policy).
- **Evidence:** In a viva, “the number came from **this** `run_id`” is stronger if **`raw_payload`** still exists to **inspect**.

---

## 7. Why cleaning is not done in Bronze

**By design:** Bronze’s job is **faithful capture** + **lineage**. Cleaning implies **choices** (stopwords, regex, dedupe rules)—those choices **change**; if you bake them into Bronze, you **cannot** re-derive a **different** Silver without **re-ingesting** from the API. **`transform_silver`** explicitly reads **`raw_payload`** and applies **project rules** (`transform_silver.py` uses `row.get("raw_payload", {})`). **Separation of concerns:** Bronze = **what they said**; Silver = **what we decided it means**.

---

## 8. Why Bronze is necessary for auditability and replay

- **Auditability:** **`run_id`** + **`bronze_data_file`** path + **`manifest.json`** tie a **batch** to **record_count**, **incremental_filter** diagnostics, and **fetch_meta**—you can show **teachers** the **exact** file that fed a pipeline run.
- **Replay:** Change Silver dedupe or skill rules → **re-run** `transform_silver` **from the same** `raw.jsonl.gz` → **new** Silver without pretending history never happened.
- **Lineage into Gold:** Gold aggregates carry **`bronze_ingest_date`** and **`bronze_run_id`** columns (`transform_gold.py`) so **monthly** facts still **reference** which **ingest batch** contributed.

---

## 9. Current active Bronze path pattern

- **Canonical (Arbeitnow, including optional slice):** `bronze_raw_gz()` → `bronze/source=<slug>[/slice=<tag>]/ingest_date=<YYYY-MM-DD>/run_id=<id>/raw.jsonl.gz` (`paths.py`).
- **Adzuna (explicit in module):** `bronze/source=adzuna_in/ingest_date=…/run_id=…/raw.jsonl.gz` (`ingest_adzuna.py` builds path inline—same shape as `paths.py` without slice).

**Hive keys:** `source=` (and rare `slice=`), `ingest_date=`, `run_id=`.

---

## 10. How run_id and ingest_date are attached

- **`run_id`:** **`new_run_id()`** in `config.py`—UTC timestamp prefix + short UUID—created **once per ingest invocation** before writing lines (`ingest_live.py` L22–23). Every line in that file gets the **same** `run_id` (then overwritten per line in the loop is **not** done—**same batch** id).
- **`bronze_ingest_date`:** **`datetime.now(timezone.utc).date().isoformat()`** for the **batch**—same for all lines in the file (`ingest_live.py` L23).
- **Path `ingest_date=` folder:** Uses the **same** `ingest_date` string as **`bronze_ingest_date`** for Arbeitnow (`bronze_raw_gz(cfg, ingest_date, run_id)`).

**Distinction:** **`ingest_date`** partitions **storage**; **`run_id`** identifies **which** run landed in that folder (typically **one** run per folder path in normal operation).

---

## 11. Manifest / health relation to Bronze

- **`manifest.json` (co-located):** **Same directory** as `raw.jsonl.gz` (`out_path.parent / "manifest.json"`). Contains **`bronze_data_file`**, **`record_count`**, **`incremental_strategy`**, **`incremental_filter`** (diagnostics from `select_jobs_for_bronze`), **`fetch_meta`**, **`fetch_watermark_created_at_after_run`**, **`connector` watermark** inputs for the **next** run. It is the **batch receipt** for Bronze.
- **`health/latest_ingest*.json`:** **Not** inside `bronze/`—lives under **`health/`**. **Human-friendly** “last successful ingest” for **ops** and quick checks: points to **`bronze_path`** and **`manifest_path`**. Arbeitnow uses `latest_ingest.json` (or slice-specific name); Adzuna uses **`latest_ingest_adzuna_in.json`** (`ingest_adzuna.py` L77–78).

---

## 12. What can go wrong if Bronze is skipped

- **No proof:** You cannot show **what the API returned** when a teacher challenges a **parsing** decision.
- **No replay:** Silver rule changes force **new API pulls**—rates, **availability**, and **non-determinism** (jobs gone from feed) break **reproducibility**.
- **Blame ambiguity:** Bugs in **dedupe** or **skill** rules cannot be **forensically** traced to a **raw** line.
- **Incremental bugs:** Without **Bronze batches** + **state**, **`connector_state.json`** logic loses meaning—**watermarks** are grounded in **what landed**.

---

## 13. What teacher may ask about Bronze

- “Isn’t Bronze wasteful?” → **Compressed** JSONL + **selective** incremental landing; **cost** vs **audit** trade-off.
- “Why not ingest straight to Parquet?” → You’d **commit** to a **column schema** too early; **JSON** preserves **full** vendor shape.
- “How do you handle API changes?” → **`raw_payload`** survives; **envelope** **`schema_version`** can bump independently.

---

## 14. What to say while explaining Bronze in viva

“**Bronze** is our **immutable evidence layer**: each API job is one **JSON line** in **`raw.jsonl.gz`**, with **`raw_payload`** equal to the **vendor JSON** and a thin **envelope** for **`source`**, **`job_id`**, and **batch** **`run_id`**. We **don’t** dedupe or extract skills here—that’s **Silver**. We **do** write **`manifest.json`** per batch and a **`health`** pointer so we can **audit** counts and **replay** Silver when rules change. Paths are **`bronze/source=…/ingest_date=…/run_id=…`** so **S3** and **lineage** stay aligned.”

---

## 15. Key terms to remember (Bronze)

| Term | In JMI |
|------|--------|
| **`raw.jsonl.gz`** | Bronze **object** name; gzip JSONL. |
| **`raw_payload`** | Full **vendor** dict—**source of truth** inside the line. |
| **`to_bronze_record`** | Connector function building envelope + **`raw_payload`**. |
| **`manifest.json`** | Batch **receipt** next to `raw.jsonl.gz`. |
| **Incremental / `select_jobs_for_bronze`** | Filters **which** fetched rows **land** in Bronze this run—still **raw** once selected. |

---

## 16. Five likely teacher questions (Bronze)

1. **What is one line of Bronze?**  
   **Answer sketch:** Metadata + **`raw_payload`**; **`run_id`** and **`bronze_ingest_date`** tag the **batch**.

2. **Why gzip?**  
   **Answer sketch:** **Smaller** S3 storage and **fewer** egress bytes; still **line-oriented** for streaming reads.

3. **Does Bronze dedupe duplicate jobs from the API?**  
   **Answer sketch:** **No**—dedupe is **Silver** on **`job_id`**; Bronze may contain **duplicates** if the API returns them across pages/runs (Silver handles).

4. **What file proves a run happened?**  
   **Answer sketch:** **`manifest.json`** + **`raw.jsonl.gz`** path + optional **`health`** JSON.

5. **Can you rebuild Silver without the API?**  
   **Answer sketch:** **Yes**, from **Bronze** files **if retained**; that’s the **replay** point.

---

## 17. One viva-ready summary paragraph (Bronze)

**Bronze** in JMI is **compressed JSONL** (**`raw.jsonl.gz`**) under **`bronze/source=<slug>/ingest_date=<YYYY-MM-DD>/run_id=<id>/`**, where each line wraps the **full** vendor JSON in **`raw_payload`** and adds **envelope** fields—**`source`**, **`schema_version`**, deterministic **`job_id`**, **`ingested_at`**, plus **batch** **`run_id`** and **`bronze_ingest_date`** applied in **`ingest_live`** / **`ingest_adzuna`**. **No** cleaning or aggregates happen here; **incremental** logic only decides **which** fetched rows **land**, recorded in **`manifest.json`** alongside **fetch** metadata. **`health/`** JSON points to the **latest** Bronze path for **ops**. Bronze exists so you can **audit** “what the API said,” **replay** **Silver** when rules change, and carry **`bronze_run_id`** lineage into **Gold**—skipping it would **collapse** **evidence** and **reproducibility**.

---

# Silver layer (deep study note — active pipeline)

**Audience:** Personal study and viva prep.  
**Source of truth in code:** `src/jmi/pipelines/transform_silver.py` (`run`, `_merge_with_prior_silver`, `load_silver_jobs_history_union`), `src/jmi/pipelines/silver_schema.py` (normalizers, `project_silver_to_contract`, `CANONICAL_SILVER_COLUMN_ORDER`), `src/jmi/connectors/skill_extract.py`, `src/jmi/utils/quality.py` (`run_silver_checks`), `src/jmi/paths.py` (`silver_jobs_batch_part`, `silver_jobs_merged_latest`, `silver_legacy_flat_jobs_root`).  
**Uncertainty:** Exact **thresholds** you used if quality checks were ever relaxed in **your** environment; **S3** vs **local** behavior when **`JMI_DATA_ROOT`** is S3 (Silver **requires** explicit **`bronze_file`** on S3—see `transform_silver`).

---

## 1. What Silver means generally

**Silver** is the **first curated relational layer** in a medallion flow: data is **typed**, **standardized**, and **deduplicated** to a **stable grain** (here: **one row per logical job**) so downstream aggregates are **honest** and **cheap**. It is **not** raw vendor JSON anymore, but it is still **job-level**—not monthly KPIs (that is **Gold**).

---

## 2. What Silver means in this project

**JMI Silver** is **Parquet** with a **fixed column contract** (`CANONICAL_SILVER_COLUMN_ORDER` in `silver_schema.py`): **`job_id`**, **`source`**, **`title_norm`**, **`company_norm`**, **`location_raw`**, **`remote_type`**, **`skills`** (stored as **JSON array string** in Parquet), **`posted_at`**, lineage fields, etc. Each **`transform_silver.run()`**:

1. Reads **one** Bronze **`raw.jsonl.gz`** (or fails if mixed **`source`** in file).
2. **Flattens** each Bronze line by reading **`raw_payload`**.
3. **Dedupes within the batch** on **`job_id`** (`keep="first"`).
4. Runs **`run_silver_checks`**—**FAIL** aborts the run (no Parquet write on failure).
5. Builds a **merged** history (**`merged/latest.parquet`**) by unioning **prior** Silver batches and **deduping globally** on **`job_id`** with **`keep="last"`** (newer ingestion wins).
6. Writes **batch** Parquet + **merged** Parquet and **`quality/silver_quality_<ingest_date>_<run_id>.json`**.

---

## 3. What comes into Silver from Bronze

For **each** Bronze line, Silver receives:

- **`raw_payload`:** Full vendor dict—**only** input for title/company/location/tags/description.
- **Envelope:** **`source`**, **`job_id`**, **`job_id_strategy`**, **`source_slug`**, optional **`source_job_id`** (Adzuna), **`ingested_at`**, **`run_id`** (stored as **`bronze_run_id`** in Silver), **`bronze_ingest_date`**, and **`bronze_data_file`** path string.

**Path-derived lineage:** **`bronze_run_id`** and **`bronze_ingest_date`** are also parsed from the **Bronze file path** via regex (`_extract_lineage_from_bronze_path`) so the Silver output folder aligns with the Bronze batch even if a row omitted a field.

---

## 4. What exact transformations happen in Silver

**Per-row flattening** (`transform_silver.py` loop, ~L266–306):

- **`_flat_payload_fields`:** Arbeitnow uses **`title`**, **`company_name`**, **`location`**, **`tags`**; Adzuna uses **`title`**, company **`display_name`**, **`adzuna_location_for_silver`**, tags omitted (skills from text).
- **Description:** **`strip_html_description`** on payload description—**plain text** for skill matching only (not kept as a Silver column in the canonical contract).
- **Skills:** **`extract_silver_skills(tags, title, desc, extra_context=…)`** — **rule-based** allowlist/aliases/stoplist (`skill_extract.py`); Adzuna adds **`adzuna_skill_blob_context`** and **`adzuna_enrich_weak_skills`**.
- **Title:** **`normalize_title_norm`** (Arbeitnow) or **`adzuna_title_norm_for_silver`** (India-specific normalization).
- **Company:** **`normalize_company_norm`** (pipes → spaces, lowercase, trim “the ” prefix when long, etc.).
- **Location:** **`normalize_location_raw`** — includes **India-specific** city→state and alias logic for Adzuna (`silver_schema.py`).
- **Remote:** **`remote_type_for_silver`** from payload + title/description heuristics (source-aware).
- **Time:** **`posted_at_iso_from_payload`** → **`posted_at`** ISO string for Gold time-axis.

**DataFrame-level:**

- **`align_silver_dataframe_to_canonical`** — ensures legacy column names map to canonical + reapplies **`normalize_title_norm`** / **`normalize_location_raw`**.
- **`drop_duplicates(subset=["job_id"], keep="first")`** — **in-batch** dedupe.
- **`project_silver_to_contract`** — **only** canonical columns, **`skills`** forced to **JSON string** for Parquet.

---

## 5. Deduplication logic and why deterministic job_id matters

- **Bronze `job_id`** is already a **hash** from stable vendor keys (`build_stable_job_id` in connectors)—**same logical job → same `job_id`** across runs **if** the vendor fields are stable.
- **Within-batch dedupe:** `raw_df.drop_duplicates(subset=["job_id"], keep="first")` — if the **API** returned the same job twice in **one** Bronze file, Silver keeps **one** row.
- **Merged history dedupe:** `concat` old + new, **sort** by **`bronze_ingest_date`**, **`bronze_run_id`**, **`ingested_at`**, then **`drop_duplicates(subset=["job_id"], keep="last")`** — **latest** seen row for that **`job_id`** wins (re-posts / re-ingests update the merged snapshot).

**Why deterministic `job_id` matters:** If `job_id` **changed** run-to-run for the same posting, **dedupe would fail** and **monthly Gold counts would inflate**. The **`job_id_strategy`** column documents **which** hash path was used for audits.

**Quality gate:** `run_silver_checks` rejects **`duplicate_job_id`** or **`duplicate_source_key`** **after** batch dedupe—if logic is wrong, the run **fails loudly** (`transform_silver.py` L320–326).

---

## 6. Skill extraction / cleaning role in Silver

- **Not ML:** **`extract_silver_skills`** uses **allowlist**, **aliases**, **stoplist**, token/phrase match against **tags + title + description** (`skill_extract.py` docstring).
- **Purpose:** Produce a **repeatable** `skills` list for **Gold skill demand**—avoid dumping **raw tag soup** into analytics.
- **Adzuna:** **`adzuna_enrich_weak_skills`** backfills skills when tags are thin—still **rule-based**.

---

## 7. Role / location / company cleaning role in Silver

- **Role (title):** **`title_norm`** is the **analytic** job title—lowercased, whitespace collapsed, **DE gender parentheticals** stripped (`normalize_title_norm`), Adzuna-specific path when needed.
- **Employer:** **`company_norm`** for **consistent** grouping (pipes, “The …”, edge punctuation)—feeds **company_hiring_monthly** and employer analytics.
- **Location:** **`location_raw`** after **`normalize_location_raw`**—**India** gets **structured** normalization (states/UTs, city aliases) so **maps and state-level** views are not **random strings**.

**Silver does not** assign **`posted_month`**—that happens in **Gold** via **`assign_posted_month_and_time_axis`** on Silver-derived DataFrames (`gold_time.py` used from Gold path; Silver stores **`posted_at`** string).

---

## 8. Lineage fields preserved in Silver

Canonical columns include:

- **`bronze_run_id`**, **`bronze_ingest_date`**, **`bronze_data_file`** — tie each Silver row to **which** Bronze batch produced it.
- **`job_id_strategy`** — audit trail for id computation.
- **`ingested_at`** — when the Bronze line was created.

These propagate into **Gold** aggregates as **`bronze_run_id`** / **`bronze_ingest_date`** on fact rows (`transform_gold.py`).

---

## 9. Current active Silver path pattern

- **Per batch:** `silver/jobs/source=<slug>/ingest_date=<YYYY-MM-DD>/run_id=<bronze_run_id>/part-00001.parquet` (`silver_jobs_batch_part` / `paths.py`).
- **Optional Arbeitnow slice:** `silver/jobs/source=arbeitnow/slice=<tag>/…` when **`JMI_ARBEITNOW_SLICE`** is set.
- **Merged:** `silver/jobs/source=<slug>/merged/latest.parquet` (`silver_jobs_merged_latest`).

---

## 10. Why merged/latest.parquet exists

**Gold** often needs **broad history** of jobs across **many** `run_id`s to compute **monthly** **`posted_month`** spans. **`merged/latest.parquet`** is a **single-file** “**current best view**” of all Silver jobs for that **source** after **global** dedupe (**last** win). **`_merge_with_prior_silver`** either **concatenates** with the previous merged file or **replaces** it with a **richer union** from **`load_silver_jobs_history_union`** when the union has **better month coverage** (see `_silver_month_span_metrics` comparison, L229–234).

**Practical effect:** Downstream **`transform_gold`** can point **`merged_silver_file`** at this path for **one** read instead of stitching dozens of batch files—while **batch** Parquet still **proves** what **this** ingest run produced.

---

## 11. Why source-prefixed Silver layout matters

- **Isolation:** `arbeitnow` vs `adzuna_in` **never** share the same **`silver/jobs/`** subtree—**no** accidental **cross-source** dedupe.
- **Glue / Athena:** External table **`jmi_silver_v2.arbeitnow_jobs_merged`** **LOCATION** targets **`silver/jobs/source=arbeitnow/merged/`** (`ddl_silver_v2_arbeitnow_merged.sql`)—**path** makes **multi-source** lakes **inspectable** in **S3 console**.

---

## 12. What old flat layout issue existed and why it was problematic

- **Legacy Arbeitnow** sometimes wrote **`silver/jobs/ingest_date=…/run_id=…`** **without** **`source=`**—**asymmetric** with Adzuna’s modular paths (`STORAGE_LAYOUT_MULTISOURCE.md`, `paths.py` comments).
- **Problem:** **Harder** to list “all EU Silver” vs “all India Silver”; **union** logic in **`load_silver_jobs_history_union`** must **explicitly** pull **`silver_legacy/jobs/…`** for old Arbeitnow batches (see **S3** branch L151–161, **local** L195–198).
- **Why current layout is better:** **One pattern** per source: **`source=<slug>/ingest_date=…/run_id=…`**—**clear partitions**, **clear IAM**, **clear** mental model for **viva**.

---

## 13. How Silver supports Gold and analytics views

- **Gold** reads **merged** and/or **history union** Silver DataFrames (`transform_gold.py` `_resolve_silver_dataframe`) and aggregates by **`posted_month`**, skill, role, location, company—**small** Parquet out.
- **`jmi_silver_v2`** Glue tables (e.g. **merged** Arbeitnow) and **`jmi_analytics_v2`** views (e.g. **`v2_eu_silver_jobs_*`**) **join** Silver-grain rows to **Gold** for **Sankey**, **long** skill rows, etc.—**Silver** supplies **row-level** truth when **aggregates** are not enough.

---

## 14. What to say while explaining Silver live

“**Silver** reads **Bronze `raw_payload`** and outputs **strict Parquet**: **one row per `job_id`** after **dedupe**. I **normalize** title, company, and location, extract **skills** with **rules**—not free-form NLP—and I **keep** **`bronze_run_id`** and **`bronze_data_file`** for lineage. Each batch lands under **`silver/jobs/source=…/ingest_date=…/run_id=…`**, and I rebuild **`merged/latest.parquet`** so **Gold** sees the **full** job history with **newest** row per **`job_id`**. **Quality checks** must **PASS** or Silver **fails**—no silent bad data.”

---

## 15. Key terms to remember (Silver)

| Term | Meaning |
|------|---------|
| **`project_silver_to_contract`** | Strip to **canonical** columns; **`skills`** → JSON **string**. |
| **`keep="first"` vs `keep="last"`** | **Batch** dedupe first row; **merged** dedupe **newest** ingestion. |
| **`run_silver_checks`** | **Blocking** validation: missing title/company, duplicate keys. |
| **`load_silver_jobs_history_union`** | All batch Parquet files for a **source** (+ legacy paths)—input to **merge** / **Gold** history. |
| **`silver_legacy/`** | Old **flat** Arbeitnow batches—**union** only for **backward compatibility**. |

---

## 16. Five likely teacher questions (Silver)

1. **Why dedupe in Silver and not Bronze?**  
   **Answer sketch:** Bronze is **lossless capture**; duplicates may **reflect** the API. Silver **enforces** **one row per job** for **honest** aggregates.

2. **Why `keep="last"` in merged?**  
   **Answer sketch:** **Re-listed** jobs should contribute **latest** title/location/skills from the **most recent** successful ingest.

3. **Is skill extraction ML?**  
   **Answer sketch:** **No**—**allowlist** / **rules** in **`skill_extract.py`**; reproducible and **cheap**.

4. **What proves Silver ran for a batch?**  
   **Answer sketch:** **`part-00001.parquet`** path + **`silver_quality_*.json`** with row counts and **`source_bronze_file`**.

5. **Can two sources share one Silver file?**  
   **Answer sketch:** **No**—Bronze batch **rejects** mixed **`source`**; paths are **`source=`**-partitioned.

---

## 17. One viva-ready summary paragraph (Silver)

**Silver** transforms **Bronze** into **canonical Parquet** job rows: for each line it reads **`raw_payload`**, derives **`title_norm`**, **`company_norm`**, **`location_raw`**, **`remote_type`**, **`posted_at`**, and **rule-based** **`skills`**, then **dedupes** on **`job_id`** within the batch and **runs** **`run_silver_checks`** before writing **`silver/jobs/source=<slug>/ingest_date=…/run_id=…/part-00001.parquet`**. **`merged/latest.parquet`** unions **prior** batches (and **legacy** flat paths for Arbeitnow where needed), **sorts** by ingest lineage, and **keeps the last** row per **`job_id`** so **Gold** and **analytics views** see a **coherent** multi-run history. **Deterministic** **`job_id`** from Bronze makes **dedupe** **stable**; **`bronze_run_id`** / **`bronze_data_file`** preserve **audit** into **Gold**. The **flat** **`silver/jobs/ingest_date=`** layout was **problematic** because it **hid** **multi-source** clarity—**active** writers use **`source=`** prefixes.

---

# Gold layer (deep study note — active pipeline)

**Audience:** Personal study and viva prep.  
**Source of truth in code:** `src/jmi/pipelines/transform_gold.py`, `src/jmi/pipelines/gold_time.py` (`assign_posted_month_and_time_axis`), `src/jmi/paths.py` (`gold_fact_partition`, `gold_latest_run_metadata_file`), `infra/aws/athena/ddl_gold_*.sql` (partition projections + **LOCATION**), `infra/aws/athena/analytics_v2_adzuna_kpi_slice.sql`, `analytics_v2_eu_kpi_slice.sql` (example view dependencies).  
**Uncertainty:** Exact **which** views sit on **`dea final 9`** (QuickSight); **incremental** `JMI_GOLD_INCREMENTAL_POSTED_MONTHS` vs **full** rebuild in **your** live runs.

---

## 1. What Gold means generally

**Gold** is the **curated, analytics-ready** layer: **pre-aggregated** tables at a **stable grain** (here: **one row per dimension value per month** + lineage) so **BI tools** and **SQL** do not re-scan **job-level** history on every query. It trades **storage of extra Parquet** for **predictable query cost** and **clear metric definitions**.

---

## 2. What Gold means in this project

**JMI Gold** writes **five fact families** plus **two kinds of metadata**, all under **`gold/`** with **Hive-partition-style** keys **`source=`**, **`posted_month=`**, **`run_id=`** (v2 layout in `paths.py`):

| Output | Purpose |
|--------|---------|
| **`skill_demand_monthly`** | Skill tag → **distinct job** counts per month |
| **`role_demand_monthly`** | Normalized **job title** → job counts per month |
| **`location_demand_monthly`** | Normalized **location** → job counts per month |
| **`company_hiring_monthly`** | Normalized **employer** → job counts per month |
| **`pipeline_run_summary`** | **One row per `posted_month`** rebuilt in this Gold run: **row counts** + **PASS** + lineage |
| **`latest_run_metadata_*`** | **Single-row** Parquet **`run_id`** pointer under **`gold/source=<slug>/latest_run_metadata/`** (Glue tables **`latest_run_metadata_arbeitnow`** / **`latest_run_metadata_adzuna`**) |

**Body columns** in fact tables include **`bronze_ingest_date`**, **`bronze_run_id`** (here **set to the pipeline `run_id` / `prid`**, not per-job Bronze ids—see §7), **`time_axis`**, and **`source`** in the **Parquet** (DDL comments note path + body alignment).

---

## 3. Why Gold exists instead of querying Silver directly in dashboards

- **Scan size:** Silver is **one row per job** across **many** batches; **`COUNT` / `GROUP BY`** on every dashboard refresh would **re-read** a large Parquet repeatedly.
- **Consistent definitions:** **Skill explode**, **title normalization**, **location grouping** are **implemented once** in **`transform_gold.py`**—**QuickSight** does not re-implement **explode** + **nunique** consistently across visuals.
- **Partition-friendly access:** Gold files are **small** and keyed by **`posted_month`** + **`run_id`** + **`source`**—**Athena** **partition projection** + **filters** match **cost_guardrails.md** (Gold-first).
- **Multi-tenant BI:** **`dea final 9`**-style dashboards use **datasets** bound to **views** (`jmi_analytics_v2`) that **expect** **Gold** fact grain—not raw Silver.

---

## 4. How posted_month is derived and why it matters

**Derivation:** `assign_posted_month_and_time_axis` (`gold_time.py`) parses **`posted_at`** (ISO, with **epoch** fallbacks for Arbeitnow quirks), takes **calendar month** **`YYYY-MM`**. If **`posted_at`** is missing/unparseable, it **falls back** to **`bronze_ingest_date[:7]`** and sets **`time_axis`** to **`ingest_fallback`** (honest labeling).

**Why it matters:** **Dashboards** answer “**labor market** activity in **March**,” not “**batch we ingested** in March.” **`posted_month`** is the **Gold partition key**; **`ingest_date`** tracks **your** pipeline clock (Silver/Bronze paths).

**Filtering:** `run()` **drops** Silver rows with **invalid** `posted_month` before aggregation (`transform_gold.py` L278–281).

---

## 5. Why Gold still keeps source and run_id concepts

- **`source`:** **Multi-source lake**—EU vs India **must not** mix in one **partition path**; Athena **`WHERE source = 'arbeitnow'`** or **`adzuna_in`** matches **partition** columns. **Parquet body** also carries **`source`** (writer explicitly sets it, L312–316) for **legacy/compat** and **joins** in SQL.
- **`run_id` (partition):** **`pipeline_run_id`** (`prid`) — typically **`JMI_PIPELINE_RUN_ID`** env or **latest** `bronze_run_id` from ordered Silver (L283–286). **Every** Gold partition for a **pipeline execution** shares this **`run_id`** so **Athena** can **`WHERE run_id = …`** to show **one** Gold build. **Not** the same as **per-job** `bronze_run_id` in Silver—**summary** table **labels** the column **`bronze_run_id`** but stores **`prid`** (L338–339).

---

## 6. Current active Gold path pattern

From **`gold_fact_partition`** (`paths.py`):

`gold/<table>/source=<slug>/posted_month=<YYYY-MM>/run_id=<pipeline_run_id>/part-00001.parquet`

**Metadata pointer (no `table` in the middle):**

`gold/source=<slug>/latest_run_metadata/part-00001.parquet`

**Optional:** **`slice=<tag>`** root for Arbeitnow experiments (`gold_root_effective`).

---

## 7. Each Gold output explained deeply

### `skill_demand_monthly`

- **Logic:** `_build_monthly_skill`: parse **`skills`** JSON from Silver → **explode** one row per (job, skill tag) → **`groupby(skill).nunique(job_id)`** → column **`job_count`** = **distinct jobs** listing that skill in that month (`transform_gold.py` L163–177).
- **Why:** **Skill mix** charts and **comparison** views need **tag demand** without **double-counting** jobs per tag explosion incorrectly—**nunique** is the **distinct job** definition.
- **Columns:** `skill`, `job_count`, `bronze_ingest_date`, `bronze_run_id` (= **`prid`**), `time_axis`, plus **`source`** in body.

### `role_demand_monthly`

- **Logic:** `_build_monthly_role`: uses **`title_norm`** (or fallbacks), lowercases/strips, **groupby(role).size()** → **`job_count`** (row count per title bucket in **Silver** for that month—each Silver row is **one job**).
- **Why:** **Histograms**, **Pareto**, **role** KPIs at **raw title** grain (before **`role_group`** analytics in views).

### `location_demand_monthly`

- **Logic:** `_build_monthly_location`: **`normalize_location_raw`** again on **`location_raw`**, drop empty, **groupby(location).size()**.
- **Why:** **Geo** and **location** demand **maps** / **bars**—**consistent** string after Silver + **second** Gold normalization pass.

### `company_hiring_monthly`

- **Logic:** `_build_monthly_company`: **`company_norm`**, lower/strip/whitespace, **groupby(company_name).size()**.
- **Why:** **Employer concentration** (raw employer grain; **“clean top15”** rollups live in **`jmi_analytics_v2`** views, not necessarily in this Gold table alone).

### `pipeline_run_summary`

- **Logic:** One **DataFrame** per **`posted_month`** with **one row**: **`source`**, **`bronze_ingest_date`** (= **`rep_date`** = max **`bronze_ingest_date`** in that month slice from Silver), **`bronze_run_id`** (= **`prid`**), **`skill_row_count`**, **`role_row_count`**, **`location_row_count`**, **`company_row_count`**, **`status`** (`PASS`), **`time_axis`** (`transform_gold.py` L334–355).
- **Why:** **Operational proof** for the dashboard sheet-2-style narrative: **counts** of **aggregate rows** produced per month for that **pipeline run**, not “number of jobs” as **skill_row_count** (that is **rows in skill_agg**, i.e. **distinct skills** with ≥1 job—**do not** misread as job count).

### `latest_run_metadata_arbeitnow` / `latest_run_metadata_adzuna`

- **Not** separate code paths—**same** write: **`gold_latest_run_metadata_file(cfg)`** → **`DataFrame([{"run_id": prid}])`** to **`gold/source=<slug>/latest_run_metadata/part-00001.parquet`** (`transform_gold.py` L373–375).
- **Glue:** Two **external tables** (`ddl_gold_latest_run_metadata_arbeitnow.sql`, `ddl_gold_latest_run_metadata_adzuna.sql`) with **different** **LOCATION** prefixes—**EU** and **India** runs **do not overwrite** each other’s pointer.
- **Why:** **Athena views** (e.g. **`v2_eu_kpi_slice_monthly`**, **`v2_in_kpi_slice_monthly`**) **`SELECT run_id FROM … latest_run_metadata_* LIMIT 1`** to filter **latest** `run_id` for **that** region.

---

## 8. What each dataset represents row-wise

| Table | One row means |
|-------|----------------|
| **skill_demand_monthly** | One **skill tag** in **`posted_month`**, **`run_id`**, **`source`**, with **`job_count`** = **distinct jobs** with that tag. |
| **role_demand_monthly** | One **normalized title string** (`role`) in that month/run/source with **`job_count`** jobs. |
| **location_demand_monthly** | One **location string** bucket with **`job_count`**. |
| **company_hiring_monthly** | One **company name** bucket with **`job_count`**. |
| **pipeline_run_summary** | **One row per `posted_month`** rebuilt—**validation** row counts for that **Gold run**. |
| **latest_run_metadata_*** | **Exactly one row** (`run_id`)—**which** Gold **`run_id`** is **current** for that **source**. |

---

## 9. Which views / dashboards depend on these Gold outputs

- **Repo-backed (Athena):** **`jmi_gold_v2.skill_demand_monthly`**, **`role_demand_monthly`**, etc., are **joined** by **`jmi_analytics_v2`** views—examples: **`analytics_v2_eu_kpi_slice.sql`**, **`analytics_v2_adzuna_kpi_slice.sql`** reference **`latest_run_metadata_*`** + **`skill_demand_monthly`**.
- **Comparison:** **`ATHENA_VIEWS_COMPARISON_V2.sql`** lineage reads **`role_demand_monthly`** / **`skill_demand_monthly`** for **EU vs India** comparisons.
- **QuickSight:** **`dea final 9`** binds **datasets** to these views/tables—**exact** visual wiring is **console-only** (see **Uncertainty**).

---

## 10. Why latest_run_metadata exists

Without it, **every** “latest dashboard” query would need **`MAX(run_id)`** over **all** S3 prefixes or **risk** picking the **wrong** region’s run. The **pointer** file **freezes** “**this** is the **`run_id`** we treat as **current** for **arbeitnow** vs **adzuna_in**,” and **views** **`SELECT`** it **once**—**matches** `ATHENA_VIEWS.sql` / v2 KPI patterns.

---

## 11. Why pipeline_run_summary exists

- **Validation artifact:** **Row counts** of **each** fact table’s **output** for **each **`posted_month`** in the run—helps **debug** empty **Gold** partitions (e.g. **no skills** exploded).
- **Teacher-facing proof:** “**PASS**” and **counts** support **data-ops** / **sheet 2** narrative in **`DASHBOARD_SPEC.md`**-style builds.
- **Not a substitute for job counts:** **skill_row_count** is **number of skill rows** in **`skill_agg`**, not **jobs in Silver**.

---

## 12. What to say in viva while explaining Gold

“**Gold** is **monthly** **Parquet** **facts** partitioned by **`source`**, **`posted_month`**, and **`run_id`**. I **slice Silver** by **`posted_month`** from **`posted_at`** (with **fallback** labeled **`ingest_fallback`**). For **skills** I **explode** tags and count **distinct **`job_id`** per skill. For **roles/locations/companies** I **group** normalized strings. **`pipeline_run_summary`** is my **per-month** **QA** row for that **run**; **`latest_run_metadata`** is a **one-row pointer** so **Athena** views can filter **`run_id`** for **EU** vs **India** without scanning everything. **I don’t** query **Silver** in **QuickSight** for standard KPIs because **Gold** is **small** and **definitionally stable**.”

---

## 13. Key terms to remember (Gold)

| Term | Meaning |
|------|---------|
| **`prid` / `pipeline_run_id`** | **Gold partition** identity for **one** transform run; **written** to **`bronze_run_id`** column in **fact** rows (pipeline id, not per-job Bronze). |
| **`rep_date`** | **`_rep_date_for_month`**: **`max(bronze_ingest_date)`** in the **month slice**—represents **lineage** stamp on aggregates. |
| **Incremental Gold months** | **`JMI_GOLD_INCREMENTAL_POSTED_MONTHS`** or **`JMI_GOLD_FULL_MONTHS`**—**Lambda** defaults **incremental** window via `default_incremental_posted_months_live_window()` in **gold handler**. |
| **`time_axis`** | **`posted`** vs **`ingest_fallback`** vs **`mixed`**—honesty when **`posted_at`** was missing. |

---

## 14. Five likely teacher questions (Gold)

1. **Is `job_count` in skill_demand_monthly “jobs” or “tags”?**  
   **Answer sketch:** **Distinct jobs** per **skill** (`nunique(job_id)` after **explode**); a job with **5 skills** contributes **once** per skill.

2. **Why does `bronze_run_id` in Gold not match Silver’s per-row Bronze id?**  
   **Answer sketch:** Column **stores** **`pipeline_run_id` (`prid`)** for **partition** alignment—see **`transform_gold.py`**—**naming** is **legacy**; **explain** clearly in viva.

3. **What if `posted_at` is missing?**  
   **Answer sketch:** **`assign_posted_month_and_time_axis`** uses **`bronze_ingest_date` month** and **`time_axis=ingest_fallback`**.

4. **Why both facts and `pipeline_run_summary`?**  
   **Answer sketch:** **Facts** = **analytics**; **summary** = **row-count proof** and **status** per **month** for **ops**.

5. **Why two `latest_run_metadata` tables?**  
   **Answer sketch:** **Separate S3 paths** per **`source`**—**EU** and **India** **pipelines** **do not** overwrite each other’s **`run_id`**.

---

## 15. One viva-ready summary paragraph (Gold)

**Gold** aggregates **Silver** into **monthly** **Parquet** **facts** under **`gold/<table>/source=<slug>/posted_month=<YYYY-MM>/run_id=<pipeline_run_id>/`**: **skills** via **explode** + **distinct job** counts; **roles**/**locations**/**companies** via **grouped** **job** counts; **each** partition carries **`source`**, **`time_axis`**, and **lineage** timestamps. **`pipeline_run_summary`** records **per-`posted_month`** **output row counts** and **`PASS`** for **that** **run**; **`gold/source=<slug>/latest_run_metadata/part-00001.parquet`** holds **one** **`run_id`** for **Athena** **“latest”** views (**`latest_run_metadata_arbeitnow`** / **`adzuna`** in Glue). **Gold** exists so **Athena**/**QuickSight** hit **small**, **partitioned** **facts** with **stable** definitions—**not** **re-aggregating** **job-level** **Silver** on **every** dashboard load. **Exact** **`dea final 9`** **bindings** are **QuickSight**-**verified**.

---

# Pipeline execution and Lambda orchestration (deep study note — active chain)

**Audience:** Personal study and viva prep—**narrate** this if a teacher watches you **invoke** or **walk through** the live path.  
**Source of truth:** `infra/aws/lambda/handlers/ingest_handler.py`, `silver_handler.py`, `gold_handler.py`, `infra/aws/lambda/deploy_ecr_create_update.sh` (image + **handler** commands + **env**), `infra/aws/eventbridge/jmi-ingest-schedule.json`.  
**Scope:** **Three** **ECR-backed** Lambdas—**Arbeitnow** ingest as **default** scheduled path; **Adzuna** is **same image** but **not** the default **ingest** handler unless you **change** deployment (**Uncertainty**).

---

## 1. Full pipeline execution story from trigger to final output

**Narration script (happy path, AWS):**

1. **Trigger:** **EventBridge Scheduler** (if **enabled**) fires **`jmi-ingest-live`** with a small JSON **`Input`** (`infra/aws/eventbridge/jmi-ingest-schedule.json`), **or** you **Test** the function in **Lambda console** / **CLI invoke** with `{}` or the same payload.
2. **Ingest Lambda** runs **`ingest_live.run()`** with **`JMI_DATA_ROOT=s3://<bucket>`**—fetches **Arbeitnow** API, writes **`bronze/.../raw.jsonl.gz`** + **`manifest.json`**, **`health/latest_ingest.json`**, updates **`state/.../connector_state.json`**, returns **`bronze_data_file`**, **`run_id`**, **`invoke_silver`**.
3. If **`invoke_silver`** is **true** (non-empty Bronze batch), ingest **async-invokes** **Silver** with **`{"bronze_file": "s3://…/raw.jsonl.gz", "run_id": "<id>"}`**.
4. **Ingest returns 200** immediately—**Silver** and **Gold** run **asynchronously** (**`InvocationType: Event`**).
5. **Silver Lambda** reads that **Bronze** gzip, runs **`transform_silver`**, writes **`silver/jobs/.../part-00001.parquet`** + **`merged/latest.parquet`**, **`quality/silver_quality_*.json`**, then **async-invokes** **Gold** with **`silver_file`**, **`merged_silver_file`**, **`run_id`** (`silver_handler.py`).
6. **Gold Lambda** sets **incremental Gold months** (default: **previous + current** UTC month string via **`default_incremental_posted_months_live_window()`** unless event overrides), runs **`transform_gold`** with **`pipeline_run_id`** from **`run_id`**, writes **Gold** Parquet + **`latest_run_metadata`**, **`quality/gold_quality_*.json`**, then **`sync_gold_run_id_projection_from_s3()`** — **S3 List** + **Athena** **`ALTER TABLE`** to refresh **Glue** **`projection.run_id.values`**.
7. **Final output for users:** **S3** has new **Bronze/Silver/Gold** keys; **Athena** can **query** **`jmi_gold_v2`** / **`jmi_analytics_v2`**; **QuickSight** **`dea final 9`** shows new data after **dataset refresh** (SPICE) or **Direct Query**.

---

## 2. Difference between local/manual run and AWS live run

| Aspect | **Local** (`JMI_DATA_ROOT=data/` or similar) | **AWS live** |
|--------|-----------------------------------------------|--------------|
| **Trigger** | You run **`python -m src.jmi.pipelines.ingest_live`** then Silver then Gold **in order** in a shell | **EventBridge** or **manual Lambda invoke**—**async** chain after ingest |
| **Paths** | Local filesystem | **`s3://bucket/...`** — same **code**, **`DataPath`** writes via **boto3** |
| **Silver input** | Can **glob** latest Bronze file | **Must** pass **`bronze_file`** URI on **S3** (ingest handler passes it explicitly) |
| **Gold projection sync** | May run if **AWS creds** + **Athena** available | **Expected** in **gold_handler**—updates **Glue** via **Athena** |
| **Chaining** | **Synchronous**—you wait for each stage | **Async**—ingest **does not wait** for Gold; check **CloudWatch Logs** for downstream |

---

## 3. Why Lambda was chosen

- **No always-on servers** (`docs/cost_guardrails.md`)—pay **per invocation** + **short** run time.
- **Same Python** as **local** (`src/jmi/pipelines/`) in a **container image** (**Linux**, full deps)—**one** artifact (**ECR** image) for all three functions (`deploy_ecr_create_update.sh`).
- **Micro-batch** fits **job-board** polling—**not** a 24/7 stream processor.

---

## 4. Why not Glue ETL / EC2 / Step Functions as main path

- **Glue ETL / Spark:** **Overkill** for **MB–GB** Parquet; **you** already own transforms in **Python**—**no** separate **Glue job** authoring for MVP.
- **EC2:** **Always-on** cost + **patching**—violates **serverless** capstone story.
- **Step Functions:** **Three** steps with **simple** **async Lambda** invokes—**orchestration** state is **minimal**; **Step Functions** adds **cost** and **IAM** surface for **little** gain **here**. **Revisit** if you add **retries**, **human approval**, or **many** branches.

---

## 5. EventBridge Scheduler role in the pipeline

- **Role:** **Time trigger** for **`jmi-ingest-live`** only—**does not** call Silver/Gold directly (`jmi-ingest-schedule.json` targets **ingest** Lambda ARN).
- **Repo schedule:** **`rate(24 hours)`** (name **`jmi-ingest-10min`** is **misleading**—fix in console when demoing).
- **State:** JSON may say **`ENABLED`**—**your** account might **disable** it during exams—**verify** live (**Uncertainty**).

---

## 6–7. Each Lambda / handler (detailed)

### A. `jmi-ingest-live` — `handlers.ingest_handler.handler`

| Topic | Detail |
|-------|--------|
| **Purpose** | Run **Arbeitnow** **`ingest_live`**; **kick off** Silver. |
| **Entry** | **`ingest_handler.handler(event, context)`** — **event** usually **`{"trigger":"eventbridge",...}`** or `{}`; **not** used for branching in code. |
| **Main logic** | **`ingest_run()`** → if **`invoke_silver`** and **`JMI_SILVER_FUNCTION_NAME`** set → **`lambda_client.invoke`** Silver with **`InvocationType: Event`**. |
| **Reads** | **HTTP** Arbeitnow API (outbound); **optional** reads **`state/.../connector_state.json`** via ingest modules. |
| **Writes** | **S3:** Bronze **`raw.jsonl.gz`**, **`manifest.json`**; **health** JSON; **state** JSON. |
| **Triggers next** | **`jmi-transform-silver`** **async** with **`bronze_file`**, **`run_id`**. **Skips** Silver if **empty** Bronze batch (`invoke_silver: false`). |
| **AWS resources** | **Lambda** (this fn), **Lambda** (invoke Silver), **S3**, **CloudWatch Logs**; **IAM** needs **`lambda:InvokeFunction`** on Silver. |
| **Why separate** | **Network-bound** fetch + **different** **timeout/memory** (512 MB, 120 s in deploy script) vs heavier transforms. |
| **Alternatives** | **Single** fat Lambda for all three—**worse** **failure isolation** and **timeout** coupling. |
| **Limitations** | **Default** code path is **Arbeitnow only**; **15 min** Lambda max; **async** means **ingest success ≠ Gold success**—**check logs**. |

### B. `jmi-transform-silver` — `handlers.silver_handler.handler`

| Topic | Detail |
|-------|--------|
| **Purpose** | **Bronze → Silver** Parquet for **one** Bronze file. |
| **Input event** | **`bronze_file`** (S3 URI string), **`run_id`** (echoed for Gold payload). **Silver** passes **`bronze_file`** into **`transform_silver.run(bronze_file=...)`**. |
| **Main logic** | **`silver_run()`** → **`transform_silver.run`** → **`lambda_client.invoke`** Gold **async** with **`silver_file`**, **`merged_silver_file`**, **`run_id`**. |
| **Reads** | **S3** Bronze **gzip** JSONL; may read **prior** **`merged/latest.parquet`** for merge. |
| **Writes** | **S3:** batch **`part-00001.parquet`**, **`merged/latest.parquet`**, **`quality/silver_quality_*.json`**. |
| **Triggers next** | **`jmi-transform-gold`**. |
| **AWS resources** | **S3**, **Lambda** (invoke Gold), **Logs**; **1024 MB**, **180 s** timeout in deploy script. |
| **Why separate** | **CPU** heavier than ingest; **isolates** **parsing** failures from **fetch** failures. |
| **Alternatives** | **SQS** between stages—**more** moving parts; **sync invoke**—**longer** ingest **latency**. |
| **Limitations** | **Mixed source** in one Bronze file **errors** out—by design. |

### C. `jmi-transform-gold` — `handlers.gold_handler.handler`

| Topic | Detail |
|-------|--------|
| **Purpose** | **Silver → Gold** facts + **refresh** **Glue** **partition projection** for **`run_id`**. |
| **Input event** | **`silver_file`**, **`merged_silver_file`** (optional), **`run_id`** (→ **`pipeline_run_id`** in **`gold_run`**), optional **`full_gold_months`** / **`incremental_posted_months`**, optional **`source_name`** (defaults **`arbeitnow`**). |
| **Main logic** | Set **`JMI_GOLD_*`** env from event; **`gold_run(...)`**; **`sync_gold_run_id_projection_from_s3()`** (**raises** on failure—**no** silent skip). |
| **Reads** | **S3** Silver Parquet (merged / union per **`transform_gold._resolve_silver_dataframe`**); **S3 List** on **`gold/role_demand_monthly/`** for projection sync. |
| **Writes** | **S3:** **Gold** fact paths, **`gold/source=<slug>/latest_run_metadata/`**, **`quality/gold_quality_*.json`**. **Athena** scratch **`athena-results/`** for **DDL**. |
| **Triggers next** | **None**—end of **chain**. |
| **AWS resources** | **S3**, **Athena** (**`StartQueryExecution`**), **Glue** (metadata via Athena **ALTER**), **Logs**. |
| **Why separate** | **Heaviest** stage; **different** **failure** mode (aggregation + **Athena**); **needs** **Athena** permissions **beyond** sample IAM JSON (**Uncertainty**). |
| **Alternatives** | **Glue Crawler** after Gold—**not** used; **manual** DDL only. |
| **Limitations** | **India Gold** needs **`source_name=adzuna_in`** in **event** or **`JMI_SOURCE_NAME`** env—**default** deploy env in script is **only** bucket roots—**Adzuna** may be **manual**/**separate** invoke (**Uncertainty**). |

---

## 8. What happens in S3 after each Lambda runs

| After… | **New/updated S3 keys (conceptually)** |
|--------|----------------------------------------|
| **Ingest** | **`bronze/.../raw.jsonl.gz`**, **`manifest.json`**, **`health/`**, **`state/`** |
| **Silver** | **`silver/jobs/source=.../ingest_date=.../run_id=.../part-00001.parquet`**, **`merged/latest.parquet`**, **`quality/silver_quality_*.json`** |
| **Gold** | **`gold/<table>/source=.../posted_month=.../run_id=.../part-00001.parquet`**, **`gold/source=<slug>/latest_run_metadata/part-00001.parquet`**, **`quality/gold_quality_<prid>.json`**, **Athena** results under **`athena-results/`** |

---

## 9. What Athena / Glue see after the pipeline completes

- **Glue Catalog:** **Table** metadata unchanged **until** **`sync_gold_run_id_projection_from_s3`** runs—**then** **`projection.run_id.values`** on **`jmi_gold_v2`** fact tables **includes** the **new** **`run_id`**.
- **Athena:** Queries **`SELECT … WHERE run_id = '…'`** and **`posted_month`** filters **return** **new** **Gold** rows; **`jmi_analytics_v2`** views that filter **“latest”** via **`latest_run_metadata_*`** **resolve** to the **new** pointer **after** **`latest_run_metadata`** Parquet **overwrite**.

---

## 10. What QuickSight depends on after pipeline completion

- **Data still in Athena**—QuickSight **does not** read **S3** directly for these datasets.
- **SPICE datasets:** Need **Refresh now** or **schedule**—**teacher** may see **stale** numbers until refresh.
- **Direct Query:** **Next** load hits **current** Athena **rows** (subject to **query** caching behavior).
- **`dea final 9`:** **Dataset** → **analysis** → **dashboard**—**no** automatic link to **Lambda** completion (**Uncertainty**: exact **refresh** setup).

---

## 11. What I should say while running the pipeline in front of teacher

“I’m triggering **`jmi-ingest-live`**. It **only** runs **Arbeitnow** fetch and writes **Bronze** to **S3**. It then **fires** **Silver** **asynchronously**, so **this** screen returns **before** **Gold** finishes. I’ll open **CloudWatch Logs** for **`jmi-transform-silver`** and **`jmi-transform-gold`** to show **Parquet** writes. **Gold** ends with **Athena** **`ALTER TABLE`** to register **new** **`run_id`** values in **Glue** **projection**—without that, **Athena** would **miss** **new** partitions. After that, I’ll **refresh** **QuickSight** or run an **Athena** **smoke** query on **`jmi_analytics_v2`**. ”

---

## 12. Common failure points in live run

- **Empty Bronze** → **Silver not invoked**—**expected** if incremental filter drops everything.
- **Silver quality FAIL** → **exception** before Gold—**no** Silver Parquet or **stale** merge.
- **Gold: no readable Silver** → **`FileNotFoundError`** from **`_resolve_silver_dataframe`**.
- **Projection sync failure** → **Gold Lambda** **errors** (handler **re-raises**)—**Athena** **won’t see** **new** **`run_id`** until fixed.
- **IAM:** Missing **Athena** on **Gold** role—**sync** fails (**Uncertainty** vs **live** policy).
- **Async chain:** **Ingest** **200** but **Gold** **failed**—**always** check **downstream** logs.

---

## 13. Key terms to remember

| Term | Meaning |
|------|---------|
| **`InvocationType: Event`** | **Async** Lambda invoke—**caller** does **not** wait for **downstream** **result**. |
| **`JMI_DATA_ROOT` / `JMI_BUCKET`** | **Lambda** env: data lake root **URI** + bucket name for **projection** helper. |
| **`sync_gold_run_id_projection_from_s3`** | **Post-Gold** **Glue** **maintenance** via **Athena** **DDL**. |
| **`invoke_silver: false`** | **Ingest** result when **Bronze** batch is **empty**—**Silver** **skipped**. |

---

## 14. Five likely teacher questions

1. **Does ingest wait for Gold?**  
   **Answer sketch:** **No**—**async** invokes; **use** **logs** or **S3** keys to **verify** **completion**.

2. **Why not one Lambda?**  
   **Answer sketch:** **Isolation**, **different** **timeouts/memory**, **clear** **failure** **boundaries**.

3. **What if EventBridge fires twice?**  
   **Answer sketch:** **Two** **runs** → **two** **`run_id`s** → **two** **partition** **folders**—**idempotence** is **per** **batch**, not **global** **dedupe** across **runs** at **Bronze**.

4. **What must happen after Gold for BI?**  
   **Answer sketch:** **Projection** **sync** + **QuickSight** **refresh** (if **SPICE**).

5. **How would you run India (Adzuna) on Lambda?**  
   **Answer sketch:** **Same** **image**; **different** **handler** or **invoke** **`ingest_adzuna`** / **Gold** with **`source_name=adzuna_in`**—**default** **scheduled** **ingest** is **Arbeitnow** (**README**).

---

## 15. One viva-ready summary paragraph (pipeline / Lambda)

**Pipeline** **execution** on **AWS** is **three** **ECR-backed** **Lambdas**: **`jmi-ingest-live`** runs **`ingest_live`**, writes **Bronze** to **S3**, and **asynchronously** invokes **`jmi-transform-silver`** with the **Bronze** **URI**; **Silver** reads **JSONL.gz**, writes **Silver** **Parquet** and **`merged/latest.parquet`**, then **async-invokes** **`jmi-transform-gold`** with **Silver** paths and **`run_id`**; **Gold** runs **`transform_gold`** (default **incremental** **posted months** in **live**), writes **Gold** **facts** and **`latest_run_metadata`**, then **`sync_gold_run_id_projection_from_s3`** **lists** **S3** **`run_id`s** and **runs** **Athena** **`ALTER TABLE`** so **Glue** **partition** **projection** **matches** **reality**. **EventBridge** **Scheduler** can **trigger** **ingest** on a **cadence**; **QuickSight** **needs** **dataset** **refresh** after **new** **data**. **Glue** **ETL**, **EC2**, and **Step Functions** are **not** the **primary** **path** because **Python** **transforms** are **already** **owned** in **`src/jmi`** and **orchestration** is **minimal**.

---

# Glue Data Catalog, Athena, manual DDL, and metadata (deep study note — v2)

**Audience:** Personal study and viva prep.  
**Voice:** As if *you* **owned** metadata: **Athena console** or **`deploy_athena_v2.py`**, **`CREATE`/`ALTER`/`CREATE VIEW`**, **`ALTER TABLE … SET TBLPROPERTIES`** after Gold runs, **not** “the crawler figured it out.”  
**Scope:** **Active v2** only: **`jmi_silver_v2`**, **`jmi_gold_v2`**, **`jmi_analytics_v2`**. Legacy **`jmi_gold` / `jmi_analytics`** — brief contrast only.  
**Repo touchpoints:** `infra/aws/athena/ddl_gold_*.sql`, `ddl_silver_v2_*.sql`, `analytics_v2_*.sql`, `scripts/deploy_athena_v2.py`, `src/jmi/aws/athena_projection.py`, `scripts/athena_smoke_v2.py`.  
**Uncertainty:** Exact **workgroup** name in your account; **whether** every **analytics** SQL file is deployed in **your** Glue catalog; **bucket** in **LOCATION** clauses.

---

## 1. What Glue Data Catalog is in general

The **AWS Glue Data Catalog** is a **central Hive-compatible metastore**: **databases**, **tables**, **partitions** (physical or **projected**), **columns**, **serde** hints, and **table properties**. **Athena**, **EMR**, **Glue ETL**, and other engines can **attach** to the **same** catalog so **one** schema definition **serves** many tools. **It does not store your Parquet bytes**—those stay in **S3**.

---

## 2. What exact part of Glue is used in this project

- **Glue Data Catalog** only: **databases** **`jmi_gold_v2`**, **`jmi_silver_v2`**, **`jmi_analytics_v2`** and their **table/view** definitions.
- **Table metadata** for **external** tables: **`LOCATION`**, **columns**, **`STORED AS PARQUET`**, **`TBLPROPERTIES`** for **partition projection**.
- **Views** stored in the catalog (**`jmi_analytics_v2`**) as **logical** objects whose definitions are **SQL** text evaluated at query time by **Athena**.

**Not** the primary story: **Glue Crawler**-driven discovery, **Glue ETL jobs** as the **transform** engine (see §3).

---

## 3. What part of Glue is intentionally not used as main path

- **Glue Crawler** as **authoritative** table creation for **Gold/Silver** fact paths.
- **Glue ETL jobs** for **Bronze→Silver→Gold** (transforms are **Python** in **Lambda/local**).
- **Glue Data Catalog** alone does **not** run queries—**Athena** (or another engine) **does**.

---

## 4. What Athena is in general

**Amazon Athena** is a **serverless interactive SQL** service: you **register** (or **create**) tables in **Glue**, **point** at **S3**, and **pay per scanned data** (with **partition pruning** reducing scans). **DDL** such as **`CREATE TABLE`**, **`CREATE VIEW`**, **`ALTER TABLE`** runs through Athena and **updates** the **Glue** catalog.

---

## 5. What Athena is doing specifically in this project

- **Query engine** for **ad-hoc** validation and **QuickSight** datasets (via **Athena** data source).
- **DDL deployment**: running **`infra/aws/athena/*.sql`** (manually or via **`scripts/deploy_athena_v2.py`**) to **create/replace** **v2** databases, **external** **Gold/Silver** tables, and **`CREATE OR REPLACE VIEW`** for **`jmi_analytics_v2`**.
- **Metadata maintenance**: **`sync_gold_run_id_projection_from_s3`** in **Gold Lambda** runs **`ALTER TABLE jmi_gold_v2.<fact> SET TBLPROPERTIES ('projection.run_id.values'='…')`** so **new** **`run_id`** prefixes **become queryable** without **`MSCK REPAIR`** for every batch.
- **Scratch results** under **`s3://<bucket>/athena-results/`** (query output location).

---

## 6. Why Athena is needed between S3 and QuickSight

- **QuickSight** does **not** parse arbitrary **S3** Parquet trees for **SQL** BI—it needs a **cataloged** **schema** and typically **SQL** access. **Athena** is the **standard** **serverless** **bridge**: **Glue** = **what** exists; **Athena** = **how** you **SELECT** it.
- **Views** (`jmi_analytics_v2`) encode **KPI** and **comparison** logic **once**—QuickSight **binds** to **view** names instead of **rebuilding** **joins** in the UI.

---

## 7. Why manual DDL was used

- **Your** pipeline **writes** **known** paths: **`source=`**, **`posted_month=`**, **`run_id=`**—**schema** and **partition layout** are **versioned in git** (`ddl_gold_*.sql`) **matching** `src/jmi/paths.py`.
- **Partition projection** requires **explicit** **`TBLPROPERTIES`** (**enums**, **ranges**)—a **crawler** does **not** **author** **projection** the way this design **requires**.
- **Reproducibility:** **Reviewable** SQL diffs; **rollback** = **drop/recreate** from repo (**v1** DDL archived under `archive_non_v2_ddl/` for contrast).

---

## 8. What exactly was done manually instead of Glue Crawler

- **`CREATE DATABASE`** for **`jmi_gold_v2`**, **`jmi_silver_v2`**, **`jmi_analytics_v2`**.
- **`CREATE EXTERNAL TABLE`** for each **Gold** fact and **Silver** merged table with **correct** **`LOCATION`** prefix, **column list**, **`PARTITIONED BY`**, and **`TBLPROPERTIES`** for **projection**.
- **`CREATE OR REPLACE VIEW`** (often dozens) for **KPI**, **EU/India helpers**, **comparison** SQL—files like **`analytics_v2_eu_silver_foundation.sql`**, **`ATHENA_VIEWS_COMPARISON_V2.sql`** lineage, deployed via **`deploy_athena_v2.py`** companion scripts or **Athena editor** runs.
- **Ongoing:** **`ALTER TABLE`** to append **new** **`run_id`** values to **`projection.run_id.values`** (**automated** post-Gold in Lambda, but **still** **explicit** metadata writes—not **crawler**-discovered).

---

## 9. What a Glue Crawler would have done

- **Listed** S3 prefixes, **inferred** **columns** and **partition keys** from **paths**, **registered** or **updated** tables.
- **Possibly** detected **new** partitions on schedule—**but** **would not** **reliably** set **custom** **partition projection** **enums** aligned to **your** **pipeline’s** **`run_id`** **semantics** without **extra** configuration.

---

## 10. Why manual DDL + controlled metadata was better here

- **Exact alignment** with **writers**: **`LOCATION`** matches **`gold_fact_partition`** output; **no** **schema drift** from **inference** mistakes.
- **Partition projection** **performance**: **Athena** can **plan** **partitions** without **listing** **every** **prefix** on **every** query when **predicates** match **projection** rules.
- **Enum `run_id`**: **Treating** **`run_id`** as a **closed set** in **Glue** matches **how** you **operate** the lake (**known** pipeline batches + **sync** after writes)—**crawler** **inference** would fight **that** model.
- **Views as code:** **Teacher feedback** changes **SQL** in **`jmi_analytics_v2`** **without** **rewriting Parquet**—**crawler** does **not** **manage** **views**.

---

## 11. What partition projection means in this project

**Glue** **`TBLPROPERTIES`** with **`projection.enabled=true`** tell Athena **logical** partitions exist **without** **physical** **`ADD PARTITION`** for **every** **`run_id=`** folder. **Sub-keys** in this repo typically include:

- **`projection.source.type=enum`** with **`arbeitnow,adzuna_in`**
- **`projection.posted_month`** as **date** type with **range** `2018-01`–`2035-12` (match **DDL** comments and **WHERE** clauses in views)
- **`projection.run_id.type=enum`** with **comma-separated** **`run_id`** list

**Queries must filter** on **partition columns** for **best** **pruning** (see comments in **`ddl_gold_*.sql`**).

---

## 12. Why `projection.run_id.values` matters

- **Every** **new** **Gold** **pipeline** **run** creates **new** **`run_id=`** **S3** **folders**. If **`run_id`** is **not** in the **enum**, **Athena** **may return no rows** for **that** **`run_id`** even though **Parquet** **exists**.
- **This project** **mitigates** by **`sync_gold_run_id_projection_from_s3`**: **list** **`gold/role_demand_monthly/`**, **extract** **`run_id`** **segments**, **ALTER** **all** **fact** **tables** in **`GOLD_V2_RUN_PROJECTION_TABLES`** (`athena_projection.py`).

---

## 13. Why tables/views need careful naming and deployment

- **QuickSight** datasets **bind** to **database + object** **names**—**renaming** **breaks** **datasets** unless **repointed**.
- **`deploy_athena_v2.py`** **patches** **`jmi_gold.`** → **`jmi_gold_v2.`** so **repo** **DDL** **tracks** **v1** **filenames** while **deploying** **v2**—**naming discipline** avoids **split-brain**.
- **`CREATE OR REPLACE VIEW`** **order** matters when **views** **reference** **other** **views**—**deploy** **scripts** **sequence** **minimal** then **comparison** views (`deploy_athena_v2.py` **subprocesses**).

---

## 14. Current active databases explained

### `jmi_silver_v2`

- **Physical** **external** **tables** over **Silver** **Parquet**—e.g. **`arbeitnow_jobs_merged`** **`LOCATION`** **`silver/jobs/source=arbeitnow/merged/`** (`ddl_silver_v2_arbeitnow_merged.sql`).
- **Role:** **Row-grain** **QA** and **analytics** **foundations** (e.g. **long** **skills**, **Sankey** **joins**) **without** **scanning** **Bronze**.

### `jmi_gold_v2`

- **Physical** **external** **tables** for **Gold** **facts** (**`skill_demand_monthly`**, **`role_demand_monthly`**, **`location_demand_monthly`**, **`company_hiring_monthly`**, **`pipeline_run_summary`**) plus **metadata** **pointers** **`latest_run_metadata_arbeitnow`**, **`latest_run_metadata_adzuna`** over **`gold/source=<slug>/latest_run_metadata/`**.
- **Role:** **Small** **monthly** **Parquet** **facts** + **“latest run_id”** **for** **filters**.

### `jmi_analytics_v2`

- **Views** **mostly**: **`sheet1_kpis`**, **`v2_eu_kpi_slice_monthly`**, **`comparison_*`**, **`v2_in_*`**, etc.—**SQL** **layer** **on top** of **`jmi_gold_v2`** and sometimes **`jmi_silver_v2`**.
- **Role:** **Presentation** **semantics** for **`dea final 9`** **without** **rewriting** **Gold** **files** **per** **chart** **tweak**.

**Legacy contrast:** **`jmi_gold`** / **`jmi_analytics`** (non-**_v2**) **definitions** **archived**; **v2** **parallel** **databases** **avoid** **destructive** **in-place** **migration** (`docs/MIGRATION_V1_V2.md`).

---

## 15. Why `jmi_analytics_v2` being a views-heavy database is valid

- **BI churn** is **faster** than **Parquet** **rewrites**—**views** **encode** **KPI** **math**, **latest-run** **filters**, **comparison** **windows**.
- **Separation:** **`jmi_gold_v2`** stays **stable** **facts**; **`jmi_analytics_v2`** **absorbs** **experimentation**—**Athena** **cost** is still **driven** by **underlying** **scans**; **Gold-first** **patterns** in **views** **keep** **scans** **small**.

---

## 16. Why deleting Athena metadata is different from deleting S3 data

- **`DROP TABLE` / `DROP VIEW`** in **Athena** removes **Glue** **catalog** **entries**—**S3** **objects** **remain** (**orphaned** **files**).
- **Deleting S3** **prefixes** **does not** **remove** **Glue** **rows**—**queries** **error** or **return** **empty** until **DDL** **fixed** or **data** **restored**.
- **Operational rule:** **Catalog** **matches** **reality**; **lifecycle** **policy** **governs** **S3** **retention** **separately**.

---

## 17. Why moving all views into `jmi_gold_v2` is not a good idea now

- **Blurs** **layers:** **Gold** **tables** **should** **mean** **materialized** **facts** in **S3**—**views** **are** **not** **materialized** **there**.
- **Permission & reuse:** **Clear** **database** **boundary** for **“facts”** vs **“presentation SQL”**—**easier** **to** **reason** about **what** **must** **match** **pipeline** **output** vs **what** **can** **iterate** **weekly**.
- **Migration cost:** **QuickSight** **datasets** **point** at **`jmi_analytics_v2`** **names**—**moving** **everything** **requires** **dataset** **edits** **without** **user** **benefit**.

---

## 18. What teacher may ask about Glue vs Athena vs Crawler

- **“Is Glue your database?”** → **Glue** **catalog** **only**; **data** **is** **S3** **Parquet**.
- **“Why Athena?”** → **Serverless** **SQL** **over** **the** **same** **catalog** **QuickSight** **uses**.
- **“Why no crawler?”** → **Explicit** **schema** **+** **partition** **projection** **enums** **match** **pipeline**; **crawler** **doesn’t** **own** **your** **`run_id`** **lifecycle**.

---

## 19. Key terms to remember

| Term | Meaning |
|------|---------|
| **External table** | **Metadata** **only**; **files** **live** **in** **S3** **`LOCATION`**. |
| **Partition projection** | **Glue** **properties** **so** **Athena** **plans** **partitions** **without** **per-prefix** **registration**. |
| **`deploy_athena_v2.py`** | **Automates** **v2** **DDL** **deploy** **+** **patches** **`jmi_gold`→`jmi_gold_v2`**. |
| **Workgroup** | **Athena** **workgroup** (e.g. **`primary`**) **for** **DDL** **+** **results** **location**. |

---

## 20. Five likely teacher questions

1. **How do new `run_id` folders become queryable?**  
   **Answer sketch:** **`ALTER TABLE`** **updates** **`projection.run_id.values`**—**often** **automated** **post-Gold** **Lambda**.

2. **What if you forget to update projection?**  
   **Answer sketch:** **Athena** **returns** **empty** **for** **that** **`run_id`** **filter** **even** **if** **S3** **has** **data**.

3. **Why both tables and views?**  
   **Answer sketch:** **Tables** = **materialized** **facts** **on** **S3**; **views** = **reusable** **SQL** **for** **BI**.

4. **Is Glue Crawler useless here?**  
   **Answer sketch:** **Optional** **bootstrap** **only**—**not** **trusted** **as** **source** **of** **truth** **for** **this** **layout**.

5. **How do you version metadata?**  
   **Answer sketch:** **Git**-tracked **SQL** **in** **`infra/aws/athena/`** **+** **deploy** **scripts**.

---

## 21. One viva-ready summary paragraph (Glue / Athena / DDL)

**AWS** **Glue** **Data** **Catalog** **holds** **v2** **databases** **`jmi_gold_v2`**, **`jmi_silver_v2`**, **`jmi_analytics_v2`**—**external** **tables** **pointing** **at** **S3** **Parquet** **with** **partition** **projection** **(`source`, `posted_month`, `run_id`)** **defined** **in** **manual** **DDL** **checked** **into** **the** **repo**, **not** **inferred** **by** **a** **Glue** **Crawler**. **Amazon** **Athena** **runs** **SQL** **and** **DDL** **against** **that** **catalog**, **bridging** **S3** **to** **QuickSight**; **after** **each** **Gold** **run**, **`sync_gold_run_id_projection_from_s3`** **updates** **`projection.run_id.values`** **so** **new** **`run_id`** **prefixes** **are** **visible**. **`jmi_analytics_v2`** **is** **mostly** **views**—**KPI** **and** **comparison** **logic** **lives** **in** **SQL** **so** **BI** **evolves** **without** **rewriting** **facts**. **Dropping** **Glue** **metadata** **does** **not** **delete** **lake** **files**; **facts** **and** **presentation** **stay** **separate** **databases** **on** **purpose**.

---

# Europe vs India comparison layer (deep study note — v2, `dea final 9` context)

**Audience:** Personal study and viva prep.  
**Primary SQL:** `docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql` (header: **“dea final 6 minimal set”** — same **comparison family** as **`dea final 9`**).  
**Related:** `infra/aws/athena/analytics_v2_cmp_location_hhi_monthly.sql`, `infra/aws/athena/analytics_v2_comparison_skills_per_job.sql`.  
**Deploy:** `scripts/deploy_athena_comparison_views_v2.py` — runs **`DROP VIEW`** for **pruned** obsolete comparison views, then **`CREATE OR REPLACE VIEW`** statements from **`ATHENA_VIEWS_COMPARISON_V2.sql`**, then drops a short **`obsolete_cmp`** list.  
**Physical cleanup:** `infra/aws/athena/comparison_v2_views.sql` drops legacy **`jmi_gold_v2.derived_*`** comparison tables (S3 blobs may remain orphaned).  
**Uncertainty:** Exact **visual → dataset** bindings on **`dea final 9`**; which **imported** QuickSight datasets are **unused**; whether **`v2_cmp_skills_per_job_april_2026`** is still the **fixed** month you want in demo.

---

## 1. Why the comparison layer exists in this project

The story is not only “EU hiring” or only “India hiring”—it is **contrast** between two labor markets fed by **different APIs** (`arbeitnow` vs `adzuna_in`). Gold facts are **per-`source`**, but **fair** side-by-side metrics need **shared rules**: same **`posted_month` grain**, **aligned `run_id` choice per month** (via **`MAX(run_id)`** from **`role_demand_monthly`**), and **honest disclaimers** (e.g. **`skill_demand_monthly`** tag counts are **not** deduped per job—see SQL file header). The **comparison layer** in **`jmi_analytics_v2`** encodes that logic in **views** so QuickSight does not rebuild subtle joins and filters by hand.

---

## 2. Which two sources are being compared and why

- **`arbeitnow` (Europe):** Public **Arbeitnow** job-board API—default **scheduled** Lambda ingest.
- **`adzuna_in` (India):** Adzuna India postings—second source with the **same Silver/Gold fact shape** (`source=` partitions).

**Why:** Different **regions**, different **vendor JSON**, **unified** only at **Silver/Gold**—comparison answers placement-style questions (skills, volume, concentration) with **explicit** alignment rules.

---

## 3. What “strict-common” means

**Strict-common months** = **`posted_month`** values that appear for **both** sources under the **same** “latest **`run_id`** per **`posted_month`**” rule inside **`month_bounds`** (typically **previous calendar month through current month** as `pm_min`–`pm_max` in `ATHENA_VIEWS_COMPARISON_V2.sql`).

- **`month_latest_eu` / `month_latest_ad`:** from **`jmi_gold_v2.role_demand_monthly`**, **`GROUP BY posted_month`**, **`MAX(run_id)`**, within **`month_bounds`**.
- **`intersection`:** `INNER JOIN` of EU and India on **`posted_month`** — a month is strict-common only if **both** sides have a row for that month after **`MAX(run_id)`** selection.

**Fairness:** You do not compare EU March to India April in the same strict-common row—**asymmetric** months drop out.

---

## 4. What “latest aligned benchmark” means

**View:** **`jmi_analytics_v2.comparison_benchmark_aligned_month`**.

- **`strict_intersection_latest_month`** = **`MAX(posted_month)`** from **`intersection`** — the **latest** calendar month that is **shared** by both sources (headline “aligned” month for top-20 mix).
- **Benchmark row** combines **`total_role_postings`** from **`role_demand_monthly`** (aligned **`run_id`**) with **`skill_tag_hhi`**, joined only when **`month_in_strict_intersection`** is true for the HHI side—so **HHI** is not paired with **non-overlapping** months.

---

## 5. Why month intersection matters

Without **intersection**, you could compare **EU** data for **March** and **India** data for **April**—different **economic** periods, **unfair** in viva. **Intersection** forces the **same** **`posted_month` label** on both sides **after** **`MAX(run_id)`** per source per month.

---

## 6. Why March/April issues happened historically

If **one** source’s **latest Gold run** did **not** produce a **`posted_month`** partition that the **other** already has, that month **drops** from **`intersection`**. Then **`strict_intersection_latest_month`** may be **only April**—charts look “April-only” even when you expected two months in the window. **Cause:** **coverage** + **which `run_id` won** per month—not random SQL noise (`docs/project_study_guide.md` §12).

---

## 7. Why some views were latest-month only

**`comparison_source_skill_mix_aligned_top20`** restricts to **`strict_intersection_latest_month`** (single **`MAX(posted_month)`** from **`intersection`**) so the **top-20** skill mix is one **headline** month where **both** sources overlap—not a full multi-month series in that view.

---

## 8. Why some views were strict-common monthly

**`v2_cmp_location_hhi_monthly`** filters **`location_demand_monthly`** to **`posted_month IN (SELECT … FROM intersection)`** and emits **one row per `posted_month` per source** for **every** intersection month in the window—**time series** of **location concentration** on **strict** months only (`layer_scope` = **`strict_common_month`**).

---

## 9. What HHI means in this project

**Herfindahl-Hirschman style:** for shares **`p_i`** over buckets **`i`**, **`HHI = Σ p_i²`**. **Higher** ⇒ more **mass** in **fewer** buckets. **Not** legal-market antitrust analysis—**operational** concentration on **Gold aggregates** for **skills** vs **locations** (different definitions—see §10–11).

---

## 10. Skill HHI explained deeply (project-specific)

**Source:** inlined CTEs **`skill_tag_hhi_*`** in **`comparison_benchmark_aligned_month`** (`ATHENA_VIEWS_COMPARISON_V2.sql`).

1. **`skill_tag_hhi_base`** from **`jmi_gold_v2.skill_demand_monthly`**, joined to **`month_latest_eu` / `month_latest_ad`** for **`run_id`** alignment.
2. **`tag_sum`** = **`SUM(job_count)`** per **`(source, posted_month, run_id)`** — **tag-demand mass** (file header: **not** deduped per job across tags).
3. **`p_i`** = **`job_count / tag_sum`** per skill row.
4. **`skill_tag_hhi`** = **`SUM(p_i²)`** per **`(source, posted_month, run_id)`**; **`month_in_strict_intersection`** flags intersection months.

**Viva line:** “This HHI is on **tag-mass shares**, not unique jobs per skill—that matches how **`skill_demand_monthly`** is built.”

---

## 11. Location HHI explained deeply (project-specific)

**Source:** **`jmi_analytics_v2.v2_cmp_location_hhi_monthly`**.

1. Same **`month_bounds`**, **`month_latest_*`**, **`intersection`** pattern.
2. **`loc_strict`:** **`location_demand_monthly`** rows for each source, **`posted_month`** in **`intersection`** only.
3. **`total_jobs`** per **`(source, posted_month, run_id)`**.
4. **`location_hhi`** = **`Σ (job_count / total_jobs)²`** — HHI on **job-count shares** across **location** buckets.

**Contrast:** **Location HHI** uses **job** counts by location; **skill-tag HHI** uses **tag** mass from **skill** fact—**not** interchangeable.

---

## 12. Why comparison architecture is view-heavy

Teacher feedback changes **windows**, **top-N**, and **alignment** faster than you want to **rewrite Parquet**. **Views** version in git; **Gold** stays **simple facts**. **`deploy_athena_comparison_views_v2.py`** automates **deploy + pruning**.

---

## 13. Current comparison views actually relevant to dashboard

**Minimal set (`ATHENA_VIEWS_COMPARISON_V2.sql`):**

- **`comparison_source_skill_mix_aligned_top20`** — top-20 skills by combined tag mass; shares renormalized within top-20 per source; **`alignment_kind`** = **`strict_intersection_latest_month`**.
- **`comparison_benchmark_aligned_month`** — role posting totals + **skill-tag HHI** + **`alignment_kind`**.

**Additional (separate SQL files):**

- **`v2_cmp_location_hhi_monthly`** — location HHI **per** strict-common month.
- **`v2_cmp_skills_per_job_april_2026`** — **Silver-backed** skills-per-job for **`posted_month = '2026-04'`** only (explicit **no March** in file comment).

---

## 14. Which comparison visuals are actually in `dea final 9`

**Not in git.** **Verify in QuickSight** which visual binds to **`comparison_*`** / **`v2_cmp_*`** datasets (**Uncertainty**).

---

## 15. Which comparison datasets were imported but not used

**Inventory** docs (`ATHENA_JMI_ANALYTICS_INVENTORY.md`, `QUICKSIGHT_V2_DATASET_STRATEGY.md`) warn **duplicate** datasets and **orphan** analyses—exact IDs **account-specific** (**Uncertainty**).

---

## 16. Why some comparison views were cleaned/deleted

- **`deploy_athena_comparison_views_v2.py`** runs **`drop_pruned`** (dozens of **`DROP VIEW IF EXISTS ...`** for obsolete names like **`comparison_source_month_totals`**, **`v2_march_strict_*`**, **`yearly_exploratory_*`**, **`comparison_strict_intersection_skill_demand`**, etc.)—then **`CREATE OR REPLACE`** from the current SQL file—then **`obsolete_cmp`** drops for a few more legacy comparison view names.
- **`comparison_v2_views.sql`** drops obsolete **`jmi_gold_v2.derived_*`** **physical** tables (comparison moved to **views** + native Gold facts).

---

## 17. What teacher may ask about fairness of comparison

- **Same `posted_month`** after **`intersection`** (`INNER JOIN`).
- **Same `MAX(run_id)` per month per source** from **`role_demand_monthly`**—**documented** in SQL.
- **Honest limits:** skill-tag HHI is **tag-mass**; location HHI is **job shares** by location.

---

## 18. What teacher may ask about month availability and strict-common logic

If India’s **latest run** did not include **`posted_month=2026-03`** while EU did, **March leaves `intersection`**—the SQL is **honest**, not “broken.” **Fix** is **data** (re-run **Gold** / **coverage**), not faking the join.

---

## 19. Key terms to remember

| Term | Meaning |
|------|---------|
| **`month_bounds`** | Rolling `pm_min`–`pm_max` in comparison SQL. |
| **`intersection`** | Months where **both** EU and India have **latest** `run_id` rows. |
| **`strict_intersection_latest_month`** | **`MAX(intersection.posted_month)`** — headline aligned month. |
| **`alignment_kind`** | **Transparency** label on output rows. |
| **`layer_scope`** | **Location HHI view:** `strict_common_month`. |

---

## 20. Five likely teacher questions

1. **Why can HHI differ between skill and location views?** — Different **facts** and **mass** definitions (tag vs job-by-location).
2. **Why only April sometimes?** — **`intersection`** shrinks; **`strict_intersection_latest_month`** picks **latest** **shared** month.
3. **Is top-20 skill mix “jobs”?** — **Tag-based**; disclose or use **`v2_cmp_skills_per_job_*`** for per-job **skills count**.
4. **What deploys comparison views?** — **`deploy_athena_comparison_views_v2.py`**.
5. **Why drop old views?** — **Catalog hygiene** and **avoid** QuickSight binding to **deprecated** names.

---

## 21. One viva-ready summary paragraph (comparison layer)

**Europe vs India** comparison is implemented in **`jmi_analytics_v2`** **views** on **`jmi_gold_v2`**: within **`month_bounds`**, **`month_latest_eu` / `month_latest_ad`** take **`MAX(run_id)` per `posted_month` per source**; **`intersection`** is the **inner join** of months—**strict-common** alignment. **`comparison_source_skill_mix_aligned_top20`** and **`comparison_benchmark_aligned_month`** anchor on **`strict_intersection_latest_month`** for a **single headline** shared month; **skill-tag HHI** uses **Σ(tag_share²)** on **`skill_demand_monthly`** (tag-mass semantics per file header); **`v2_cmp_location_hhi_monthly`** computes **Σ(location job share²)** for **every** intersection month. **March/April gaps** reflect **missing `posted_month` on one side**, not bad joins. **Views** iterate without rewriting Gold; **exact `dea final 9` wiring** is **QuickSight-verified**.

---

# Cost architecture, S3 request anomaly, and scheduler (deep study note)

**Audience:** Personal study and viva prep.  
**Voice:** This is a **real incident class** in **this** project’s documentation—not a generic “cloud cost 101” lecture.  
**Sources:** `docs/cost_guardrails.md`, `docs/project_study_guide.md` §16–18, `infra/aws/eventbridge/jmi-ingest-schedule.json`, `scripts/pipeline_live_sync.py`, `src/jmi/aws/athena_projection.py` (S3 **list** for **`run_id`**).  
**Uncertainty:** **Exact** dollar attribution on **your** bill; **which** single action (if any) dominated **APS3-Requests-Tier1**—**Cost Explorer + S3/CloudTrail** are required to prove **one** root cause.

---

## 1. Why cost matters in this project

**Hard cap mindset:** **`docs/cost_guardrails.md`** states **≤ $3** total spend **for the MVP**—**student / capstone** economics, not enterprise FinOps. **Every** choice (serverless, Parquet, **Gold-first** BI, **incremental** Gold months) **exists** partly so **the** **demo** **stays** **affordable**.

---

## 2. Why serverless architecture was chosen from a cost perspective

- **No idle clusters:** **Lambda + S3 + Athena + Glue catalog** **pay** **when** **used**—**aligned** with **micro-batch** **job-board** **ingestion** (not 24/7 streaming).
- **No** **always-on** **EC2** (explicitly **avoided** in **trade-off** tables in `docs/project_study_guide.md`).

---

## 3. Which AWS pieces cost money here and how

| Piece | How it shows up |
|-------|------------------|
| **S3** | **Storage** (GB-month) **+** **per-request** **charges** (**GET/PUT/LIST/HEAD**-class activity—**billing** **line** **names** **vary** **by** **region** **class**) |
| **Lambda** | **Per** **invoke** **+** **duration** **×** **memory** |
| **Athena** | **Data scanned** **per** **query**; **DDL** **results** **land** **in** **`athena-results/`** (also **S3** **writes**) |
| **QuickSight** | **Author**/**reader** **pricing** **model** **+** **SPICE** **refresh** **patterns** |
| **Glue** | **Catalog** **metadata** **cheap** **relative** **to** **data** **motion**; **crawler** **would** **add** **LIST** **cost** **if** **used** |

---

## 4. Why S3 request volume matters

S3 is **cheap** **per** **GB** **stored**, but **request** **pricing** **can** **spike** **when** **automation** **does** **many** **small** **operations**: **listing** **large** **prefixes**, **repeated** **syncs**, **HEAD** **every** **object**, **or** **tools** **polling** **the** **bucket**. **This** **project** **uses** **deep** **prefixes** (**`run_id=`** **segments**)—**naive** **full** **tree** **walks** **multiply** **requests**.

---

## 5. What APS3-Requests-Tier1 means in practical project terms

In **`docs/project_study_guide.md`**, **APS3-Requests-Tier1** names the **billing** **dimension** for **Standard** **S3** **request**-type **charges** in **`ap-south-1`**. **Practically:** **you** **saw** **(or** **could** **see**) **unexpected** **cost** **on** **S3** **requests**, **not** **only** **storage**. **It** **does** **not** **by** **itself** **say** **which** **API** **(GET** **vs** **LIST)** **dominated**—**that** **needs** **Cost** **Explorer** **breakdown** **+** **access** **patterns** (**Uncertainty**).

---

## 6. What likely caused the cost anomaly in this project

**Repo** **position** (§16): **no** **single** **guaranteed** **cause** **without** **account** **evidence**. **Plausible** **contributors** **that** **fit** **JMI** **specifically**:

- **Frequent** **`aws s3 sync`** **during** **dev** **(e.g.** **`scripts/pipeline_live_sync.py`)** **without** **tight** **excludes** **→** **many** **PUTs/GETs** **across** **bronze/silver/gold** **trees**.
- **Overly** **aggressive** **EventBridge** **cadence** **(if** **ever** **set** **faster** **than** **daily)** **→** **more** **Lambda** **invokes** **and** **more** **S3** **writes** **per** **day**.
- **Gold** **Lambda** **`collect_run_ids_from_s3_gold`** **(paginated** **`list_objects_v2`** **on** **`gold/role_demand_monthly/`**) **—** **necessary** **for** **projection** **sync**, **but** **still** **LIST** **traffic**.
- **Athena** **/** **tools** **that** **induce** **extra** **LIST** **or** **wide** **scans** **if** **partition** **filters** **missing**.

**Honest** **viva** **line:** **“We** **treated** **it** **as** **request** **volume** **on** **S3** **in** **APS3;** **we** **reduced** **automation** **frequency** **and** **rationalized** **sync** **—** **exact** **blame** **per** **API** **needs** **billing** **drill-down.”**

---

## 7. How schedule frequency affects cost

Each **scheduled** **ingest** **fires** **Lambda** **→** **writes** **Bronze** **+** **Silver** **+** **Gold** **+** **quality** **JSON** **+** **state** **updates** **+** **Athena** **DDL** **for** **projection** **sync**. **Doubling** **run** **frequency** **roughly** **doubles** **that** **churn** **(plus** **QuickSight** **refresh** **if** **tied** **to** **fresh** **data)**.

---

## 8. How `aws s3 sync` / scans / repeated writes can affect cost

- **`pipeline_live_sync.py`** **documents** **belt-and-suspenders** **`--exclude`** **patterns** (`_SILVER_SYNC_EXCLUDES`, `_GOLD_SYNC_EXCLUDES`) **so** **legacy** **`ingest_date=*`** **flat** **Silver** **or** **`ingest_month*`** **Gold** **keys** **do** **not** **get** **swept** **repeatedly**.
- **Unfiltered** **sync** **=** **many** **PUTs** **for** **unchanged** **objects** **or** **re** **listing** **huge** **prefixes**.
- **Athena** **wide** **SELECT** **without** **`source`/`posted_month`/`run_id`** **predicates** **→** **large** **scan** **bill** (different **line** **item** **than** **APS3** **requests**, **but** **same** **“death** **by** **a** **thousand** **cuts”** **mindset**).

---

## 9. Why scheduler cadence was reduced

**Documented** **in** **`project_study_guide.md`**: **moving** **from** **overly** **frequent** **triggers** **to** **daily** **(or** **disabling** **the** **schedule** **during** **validation)** **reduces** **Lambda** **+** **downstream** **S3** **writes**. **This** **is** **an** **operational** **knob** **you** **can** **pull** **without** **code** **changes**.

---

## 10. Current scheduler state and why 24 hours is safer

**Repo** **file** **`jmi-ingest-schedule.json`:** **`ScheduleExpression`:** **`rate(24 hours)`**, **`Description`** **says** **ingest→silver→gold** **every** **24** **hours**. **That** **caps** **automated** **runs** **at** **one** **per** **day** **unless** **you** **invoke** **manually**.

**Naming** **mismatch:** **`Name`:** **`jmi-ingest-10min`** **is** **historically** **misleading** **—** **the** **rate** **is** **not** **10** **minutes**. **Fix** **the** **schedule** **name** **in** **console** **when** **demoing** **so** **teachers** **don’t** **think** **you** **run** **every** **10** **minutes**.

**Why** **24h** **is** **safer** **than** **high** **frequency:** **job-board** **data** **does** **not** **need** **sub-hour** **freshness** **for** **this** **MVP**; **daily** **strikes** **balance** **between** **freshness** **and** **cost**.

---

## 11. Why this does not fully solve every cost issue

**Daily** **schedule** **does** **not** **stop** **manual** **`pipeline_live_sync`** **runs**, **heavy** **Athena** **queries**, **SPICE** **refresh** **storms**, **or** **accidental** **sync** **loops** **—** **it** **only** **limits** **one** **automatic** **source** **of** **churn**.

---

## 12. What other project actions can still raise cost

- **`JMI_GOLD_FULL_MONTHS=1`** **or** **broad** **incremental** **month** **lists** **→** **more** **Gold** **writes** **and** **bigger** **Athena** **rebuilds**.
- **Comparison** **/** **analytics** **views** **without** **partition** **filters** **in** **QuickSight** **or** **adhoc** **SQL**.
- **Leaving** **EventBridge** **ENABLED** **while** **debugging** **bad** **deploys** **→** **repeat** **failed** **Lambda** **runs** **still** **touch** **S3** **/** **logs**.
- **Lifecycle** **not** **set** **on** **old** **Bronze** **(per** **`cost_guardrails.md`)** **→** **storage** **creep**.

---

## 13. How to explain this cost incident honestly in viva

- **We** **noticed** **S3** **request**-type **spend** **(APS3-Requests-Tier1** **line)** **in** **`ap-south-1`** **—** **not** **storage** **alone**.
- **We** **did** **not** **pretend** **one** **magic** **root** **cause** **without** **Cost** **Explorer** **evidence**; **we** **listed** **plausible** **project** **behaviors** **(sync,** **schedule,** **LIST** **for** **projection,** **wide** **queries)**.
- **We** **mitigated** **operationally:** **daily** **schedule** **+** **sync** **excludes** **+** **Gold** **incremental** **defaults** **+** **partition** **discipline**.

---

## 14. What teacher may ask about cost decisions

- **“Why** **not** **free** **tier** **forever?”** → **Usage** **grows** **with** **runs** **and** **queries**; **guardrails** **matter**.
- **“Why** **daily** **and** **not** **hourly?”** → **Cost** **vs** **freshness** **for** **job** **ads** **MVP**.
- **“Did** **you** **prove** **the** **root** **cause?”** → **Honest** **answer:** **hypotheses** **+** **billing** **drill-down** **recommended**.

---

## 15. Key terms to remember

| Term | Meaning |
|------|---------|
| **APS3-Requests-Tier1** | **Billing** **line** **for** **S3** **request**-type **charges** **(guide** **wording)** **in** `ap-south-1`. |
| **Gold-first** | **BI** **hits** **small** **aggregates**, **not** **full** **Silver** **every** **refresh**. |
| **Partition pruning** | **WHERE** **on** **`source`/`posted_month`/`run_id`** **to** **cut** **Athena** **scan** **cost**. |
| **`rate(24 hours)`** | **EventBridge** **Scheduler** **expression** **in** **repo** **JSON** **for** **ingest**. |

---

## 16. Five likely teacher questions

1. **What** **was** **the** **incident?** — **Unexpected** **S3** **request** **billing** **line** **(APS3-Requests-Tier1)** **—** **mitigated** **by** **ops** **changes**, **not** **mythical** **single** **bug**.
2. **Why** **does** **sync** **matter?** — **`aws s3 sync`** **generates** **many** **requests** **on** **large** **trees**; **excludes** **in** **`pipeline_live_sync.py`** **reduce** **noise**.
3. **Why** **24** **hours?** — **Repo** **schedule** **file**; **reduces** **automated** **churn** **vs** **faster** **cadence**.
4. **Does** **Gold** **Lambda** **cost** **S3** **LIST?** — **`sync_gold_run_id_projection_from_s3`** **lists** **`gold/role_demand_monthly/`** **—** **necessary** **for** **projection** **maintenance**.
5. **What** **else** **could** **spike** **cost?** — **Athena** **wide** **scans**, **SPICE** **refresh**, **manual** **full** **Gold** **rebuilds**.

---

## 17. One viva-ready summary paragraph (cost / scheduler)

**This** **project** **targets** **a** **≤$3** **MVP** **mindset** **(`cost_guardrails.md`)** **using** **serverless** **pieces** **that** **bill** **per** **use** **—** **but** **S3** **request** **volume** **still** **matters:** **a** **documented** **incident** **class** **is** **unexpected** **APS3-Requests-Tier1** **line-item** **growth** **in** **`ap-south-1`**, **plausibly** **driven** **by** **frequent** **`aws s3 sync`**, **aggressive** **scheduling**, **or** **LIST-heavy** **automation** **(including** **Gold** **projection** **sync’s** **S3** **listing)** **—** **the** **repo** **explicitly** **refuses** **to** **invent** **one** **root** **cause** **without** **Cost** **Explorer** **evidence**. **Mitigations** **are** **architectural** **and** **operational:** **Gold-first** **queries**, **incremental** **Gold** **months**, **`pipeline_live_sync.py`** **sync** **excludes**, **and** **`jmi-ingest-schedule.json`** **set** **to** **`rate(24 hours)`** **(despite** **the** **misleading** **`jmi-ingest-10min`** **name)** **to** **limit** **automated** **daily** **churn**. **Daily** **schedule** **does** **not** **fix** **manual** **bad** **habits** **or** **wide** **Athena** **queries**.

---

# Project issues, bugs, confusion points, and fixes (deep study note)

*Framing: personal study / viva prep — this describes **real evolution** in this repo (medallion lake, v2 analytics, QuickSight), not generic software stories.*

---

## 1. Why this section matters for viva

Examiners reward **honest engineering narrative**: what broke, how you noticed, what you changed, and what you would still watch. This project’s pain is **mostly semantic and operational**, not “syntax errors”: **time** (`posted_month` vs ingest), **fair comparison** (strict-common intersection), **catalog vs S3** (Glue projection, `run_id` enum), **layout migration** (Silver flat vs `source=`), and **layered cleanup** (duplicate Athena views, QuickSight dataset sprawl, schedule/cost). Showing you can connect **symptom → root cause → fix → lesson** proves you owned the system end-to-end.

---

## 2. Major issue: March missing from some views

**What went wrong:** Dashboards or SQL “lost” **March** while **April** appeared—especially in **EU vs India** comparison paths.

**Why it happened:** Comparison logic uses an **`intersection`** of `posted_month` values where **both** sources have a **latest row per month** after `MAX(run_id)`-style selection. If **one** source’s latest Gold coverage for **`posted_month = '2026-03'`** is missing or not aligned with the other, **March drops out of intersection**. The SQL is **consistent** with “fair same-month” comparison; it is not silently mixing EU March with India April. Separately, some views (e.g. skills-per-job comparison) are **explicitly April-only** by design (`v2_cmp_skills_per_job_april_2026` — comment in SQL: fixed filter, no March).

**How it was detected:** Empty or thin rows for March in intersection-backed views; `docs/project_study_guide.md` §12 and `docs/project_master_study_book.md` call out this pattern; validating `role_demand_monthly` / `posted_month` per source in Athena.

**How it was fixed:** **Data and window** fixes, not “patching the join”: widen backfill, ensure both pipelines emit the target **`posted_month`**, adjust `incremental_posted_months` / full-month Gold when needed; use views that **list all intersection months** (e.g. monthly HHI) when the story is “time series across strict-common months,” not only `strict_intersection_latest_month` headline KPIs.

**Lesson:** **Absence** in a **fair-comparison** view is often **coverage semantics**, not a random bug—teach the **business question** (posted vs ingested month).

---

## 3. Major issue: latest-run / month logic confusion

**What went wrong:** “Latest run” KPIs disagreed across charts, or **Athena returned no rows** while S3 had Parquet.

**Why it happened:** Multiple concepts stack: **`run_id`** on paths, **`posted_month`** partitions, **`latest_run_metadata_*`** pointer files, and **Glue partition projection** (`projection.run_id`). Old Glue tables could keep **`storage.location.template`** or **`projection.run_id.type` = `injected`**, which breaks **JOIN-based** latest-run views (see `docs/aws_live_fix_gold_projection.md`). README-era text sometimes mixed **`ingest_month`** language with the current **`posted_month`** Gold layout (`src/jmi/paths.py` — facts live under `posted_month=`).

**How it was detected:** `COUNT(*)=0` in base Gold or latest views while S3 listing shows data; Athena **`CONSTRAINT_VIOLATION`** on projection; mismatch between pointer `run_id` and fact table scans.

**How it was fixed:** Glue-first: remove bad **`storage.location.template`**, set **`projection.run_id.type` = `enum`** with **updated** `projection.run_id.values` after each run; validate with `infra/aws/athena/validate_gold_projection_fix.sql`. Code path: Gold Lambda **`sync_gold_run_id_projection_from_s3`** appends new `run_id` values so new partitions are visible.

**Lesson:** **Catalog metadata** is part of the product—**same S3 paths** can be **invisible** to Athena if projection is wrong.

---

## 4. Major issue: strict-common comparison becoming “April-only”

**What went wrong:** Headline benchmark views looked like **only April** mattered, even when the team expected a **multi-month** window.

**Why it happened:** Views such as **`comparison_benchmark_aligned_month`** anchor on **`strict_intersection_latest_month`** = **`MAX(posted_month)` from intersection** (`docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql`). If intersection shrinks to **only April** (e.g. March missing on one side), the **single headline month** is April—**by design**. Other views (e.g. **`v2_cmp_location_hhi_monthly`**) filter to **all** intersection months for a **series**.

**How it was detected:** Comparing “single month” benchmark vs **monthly** strict-common views; reading `alignment_kind` / layer_scope comments in SQL.

**How it was fixed:** **Clarify the right view for the question**; fix **data coverage** for March if the business needs March in intersection; avoid interpreting the **benchmark** view as “full history.”

**Lesson:** **One** “latest shared month” KPI is **not** a substitute for **month-by-month** strict-common panels.

---

## 5. Major issue: Silver layout inconsistency

**What went wrong:** Duplicate or confusing paths under **`silver/`**—flat **`ingest_date=`** batches vs **`source=<slug>/ingest_date=.../run_id=...`**, plus optional **slice** segments for Arbeitnow.

**Why it happened:** **Evolution**: early Arbeitnow batches used a **flat** layout; multi-source design introduced **`source=`**-prefixed modular paths. **`silver_legacy`** holds archived flat batches (`src/jmi/paths.py` — `silver_legacy_flat_jobs_root`).

**How it was detected:** Wrong file picked for Gold, or docs/README pointing to old paths; `transform_silver` comments reference **legacy + slice** coexistence.

**How it was fixed:** **Canonical** modular layout for new runs; legacy quarantined under **`silver_legacy`**; config/env (`JMI_ARBEITNOW_SLICE`) isolates **slice** experiments without corrupting the main tree.

**Lesson:** **Lake layout** is a **contract**—treat renames as **migration**, not “just another folder.”

---

## 6. Major issue: Gold `run_id`, projection, and Athena visibility

**What went wrong:** New Gold runs **did not show up** in Athena, or **only literal** `WHERE run_id = '…'` worked.

**Why it happened:** Partition **projection** requires **`run_id`** values to match **actual S3 prefixes**. **`injected`** projection type can conflict with **JOIN** filters on `run_id`. Stale **`storage.location.template`** misroutes projected paths.

**How it was detected:** Documented playbook in `docs/aws_live_fix_gold_projection.md`; Lambda logs from **`sync_gold_run_id_projection_from_s3`** (`infra/aws/lambda/handlers/gold_handler.py`).

**How it was fixed:** **Enum** projection + **append** new `run_id`s; remove stale Glue properties; re-validate.

**Lesson:** **Automation** (post-Gold sync) reduces manual Glue edits but **must stay monitored** when projection fails.

---

## 7. Major issue: duplicate or unnecessary analytics views

**What went wrong:** Many **experimental** comparison and “March strict” / “yearly exploratory” view names coexisted; Glue listed **obsolete** objects.

**Why it happened:** Rapid iteration on **EU vs India** semantics—multiple drafts before settling on **`jmi_analytics_v2`** patterns.

**How it was detected:** Deploy scripts (`scripts/deploy_athena_comparison_views_v2.py`) carry explicit **`drop_pruned`** / **`obsolete_cmp`** lists; `comparison_v2_views.sql` drops legacy **`derived_*`** tables.

**How it was fixed:** **Prune** views in deploy order, then **`CREATE OR REPLACE`** from the current SQL file—catalog matches repo.

**Lesson:** **View sprawl** is **technical debt**; **deploy scripts** are the **closure** that keeps Athena **trimmed**.

---

## 8. Major issue: imported-but-unused QuickSight datasets

**What went wrong:** **QuickSight** listed datasets that **do not** power the current **`dea final 9`** (or v2) analysis—imports, duplicates, or old **`jmi_analytics`** (non-v2) bindings.

**Why it happened:** **Iteration**: v1 → v2 migration, **`create-data-set`** re-runs, **SPICE** experiments, and **dashboard copy** paths leave **orphan** datasets. `docs/dashboard_implementation/QUICKSIGHT_V2_DATASET_STRATEGY.md` warns: detach v1 / `*_latest` / duplicates; `docs/project_study_guide.md` notes inventory/duplicate risk.

**How it was detected:** Strategy doc’s **do not attach** list; row counts zero or stale SPICE; **Manage data** vs **actual analysis** wiring.

**How it was fixed:** **Operational**: rename display names (`QUICKSIGHT_V1_V2_NAMING.md`), **detach** unused from the active analysis, **avoid** deleting global datasets blindly—**not** fully automatable from git.

**Lesson:** **BI layer state** is **account-local**; repo documents **intent**, not **every** dataset ID.

---

## 9. Major issue: schedule / cost anomaly

**What went wrong:** **S3 request** billing (**APS3-Requests-Tier1** in `ap-south-1`) spiked relative to expectations.

**Why it happened:** **Operational churn**: frequent **`aws s3 sync`**, **EventBridge** cadence, **LIST**-heavy automation (including Gold projection sync listing **`gold/role_demand_monthly/`**). Repo mitigated with **`rate(24 hours)`** in `jmi-ingest-schedule.json` (name **`jmi-ingest-10min`** is **misleading**).

**How it was detected:** AWS **Cost Explorer** / billing dimensions; `docs/cost_guardrails.md` and `docs/project_study_guide.md` §16.

**How it was fixed:** **Slower** schedule, **sync excludes** (`scripts/pipeline_live_sync.py`), **guardrails** mindset—**exact** root cause per account needs **evidence**, not guessing.

**Lesson:** **Serverless** is cheap until **request volume** and **wide scans** accumulate—**cadence** is a **cost knob**.

---

## 10. Major issue: naming and metadata cleanup

**What went wrong:** **Human confusion**: schedule **name** vs **expression**, **`ingest_month`** vs **`posted_month`** in older docs, **legacy** Gold prefixes (`gold_legacy`, `comparison_*` parquet paths) still on S3.

**Why it happened:** **Schema evolution** and **demo pressure**—names lagged behavior.

**How it was detected:** Code (`paths.py`) vs README drift; **`jmi-ingest-10min`** vs `rate(24 hours)`; `legacy_comparison_gold_parquet_paths.txt` references.

**How it was fixed:** **Docs-as-code** alignment (`project_study_guide`, migration notes), **rename** in QuickSight **display** only**,** explicit **legacy** labels in paths module.

**Lesson:** **Naming** is **operability**—future you (and examiners) read **names** first.

---

## 11. Cross-issue summary (debugging and correction process)

| Issue | What went wrong | Why | Detected | Fixed | Lesson |
|-------|------------------|-----|----------|-------|--------|
| March missing | Month absent in intersection or April-only view | Dual coverage, or fixed `posted_month` in view | Athena checks, guide §12 | Coverage + right view | Fair comparison ≠ all months |
| Latest-run / month | No rows or inconsistent KPIs | Glue projection + time keys | Zero counts, `aws_live_fix` doc | Enum + template cleanup | Catalog = query path |
| April-only strict-common | Single headline month | `MAX` on intersection | SQL vs benchmark semantics | Data + interpretation | Benchmark vs series |
| Silver layout | Wrong or duplicate paths | Legacy + multi-source | Path inspection | Canonical + `silver_legacy` | Layout is contract |
| Gold projection | New runs invisible | `run_id` + projection | Lambda sync + Athena | Append enum, fix Glue | Automate + verify |
| Duplicate views | Name sprawl | Iteration | Deploy `drop_pruned` | Prune + deploy | Deploy scripts close debt |
| QS unused datasets | Orphans | v1/v2, imports | Strategy doc | Detach/rename in QS | Console truth |
| Schedule / cost | S3 request $$ | Cadence + sync | Billing | 24h + excludes | Cadence is cost |
| Naming cleanup | Misleading labels | Drift | Read vs run | Docs + display renames | Names matter |

---

## 12. What a teacher may ask about debugging and correction process

- **How did you prove** the SQL was correct before blaming data? (Intersection definition, `posted_month` presence per source.)
- **Where did you look first** when Athena returned zero rows? (Glue `LOCATION`, projection, `run_id` enum, `storage.location.template`.)
- **How do you avoid** breaking QuickSight when changing views? (Drop order, `CREATE OR REPLACE`, SPICE refresh, detach legacy datasets.)
- **What is your “definition of done”** after a Gold run? (S3 keys, projection update, smoke query, optional QS refresh.)
- **How do you explain** a month “missing” without sounding evasive? (Coverage + fairness semantics.)

---

## 13. Key terms to remember

**Intersection**, **strict_intersection_latest_month**, **`posted_month`**, **`run_id`**, **partition projection**, **`enum` vs `injected`**, **`storage.location.template`**, **`latest_run_metadata_*`**, **`silver_legacy`**, **`source=` prefix**, **`drop_pruned`**, **SPICE vs Direct Query**, **APS3-Requests-Tier1**, **`rate(24 hours)`**.

---

## 14. Five likely teacher questions

1. **Why can March disappear even when the pipeline “ran successfully”?**  
2. **Why does your benchmark view show only one month while another view shows several months?**  
3. **What is the difference between fixing Glue and fixing Parquet on S3?**  
4. **Why keep experimental views out of the catalog?**  
5. **How would you verify QuickSight matches the repo’s intended v2 dataset set?**

---

## 15. One viva-ready summary paragraph

This project’s hardest problems were **not** random bugs but **semantic alignment**: **fair EU–India comparison** requires a **strict-common intersection** of `posted_month`, so **March** can vanish when **one** source lacks that month’s latest row—**the SQL is honest**, not broken. **Athena** visibility depended on **Glue partition projection** (`run_id` **enum**, no bad **`storage.location.template`**, post-run **sync** from Lambda), while **Silver** and **Gold** paths evolved from **legacy flat** prefixes to **canonical `source=`** layouts with **`silver_legacy`** for archives. **Analytics** churn produced **duplicate Athena views**—addressed by **deploy scripts** that **prune** obsolete names before **`CREATE OR REPLACE`**—and **QuickSight** accumulated **imported** datasets that **git** cannot fully inventory; **operational** cleanup (detach, rename, strategy) completes the story. **Cost** and **schedule** tied together: **S3 request** spikes led to **slower** **`rate(24 hours)`** automation and **sync** discipline—**architecture plus ops**, not theory.

---

# Viva preparation and live demonstration (speaking script, active v2 only)

*Use this section when you are **standing in front of a teacher**: short sentences you can say out loud, in order. Scope: **current active v2** (`jmi_silver_v2`, `jmi_gold_v2`, `jmi_analytics_v2`), capstone dashboard **`dea final 9`**, sources **Arbeitnow (EU)** and **Adzuna India**.*

---

## 1. How to introduce the project in front of teacher

Open with **one sentence of purpose**, then **one sentence of stack**, then **what you will show**.

**Say:** “Good morning/afternoon. I built **Job Market Intelligence**—a small **data lake on AWS** that ingests public job APIs, cleans them into **one row per job**, aggregates to **monthly KPIs**, and visualizes **Europe versus India** in **QuickSight**. I’ll walk **S3 → Lambda → Glue/Athena → the dashboard**, and if time allows, **one live query** to prove the numbers trace back to **partitioned Parquet**.”

**Do not** open with tools (“I used Lambda”) before outcome (“job market insight with lineage”).

---

## 2. 30-second explanation

“I pull **Arbeitnow** and **Adzuna India** job ads into **S3 Bronze** as immutable **gzip JSON**, transform to **Silver Parquet** with **deduped `job_id`**, aggregate to **Gold** monthly facts by **`posted_month`** and **`run_id`**, register tables in **Glue**, query **Athena**, and chart in **QuickSight** **`dea final 9`**. The point is **traceable KPIs**: every chart can be tied to a **batch** and a **posting month**, not a black box.”

---

## 3. 1-minute explanation

Add **why medallion** and **why two regions**.

“Raw APIs are messy and change shape, so I keep **Bronze** as evidence, **Silver** as a **stable schema**, **Gold** as **small Parquet aggregates** so dashboards stay fast and cheap. I partition by **`source=`** so **EU (Arbeitnow)** and **India (Adzuna)** never collide in the lake. **Glue + Athena** expose the same files to SQL; **`jmi_analytics_v2`** holds **views** for KPIs and **EU–India comparison** math. **QuickSight** is the presentation layer—**`dea final 9`** is my named analysis/dashboard outcome.”

---

## 4. 3-minute explanation

Layer in **operations and honesty**.

1. **Ingest:** Each run gets a **`run_id`** and **`ingest_date`**; Bronze lines carry **`raw_payload`** for replay.  
2. **Silver:** Normalization, **skills** from rules + allowlists, **dedupe**.  
3. **Gold:** **`posted_month`** from posting time—answers “market in March,” not “we ingested in March.”  
4. **AWS:** **Lambda** runs the same Python as local; **S3** is source of truth; **Glue** is the catalog; **Athena** is the SQL engine; **EventBridge** can schedule **daily** automation (**repo:** `rate(24 hours)`—**Uncertainty:** live schedule may differ).  
5. **Comparison:** **Strict-common** months require **both** sources—fair, but months can **drop** if coverage differs—say that plainly in viva.

---

## 5. How to explain the AWS architecture live

**Point on a diagram or console in this order:**

| Layer | Say this |
|-------|----------|
| **S3** | “All layers land here—**Bronze gz**, **Silver/Gold Parquet**, **`latest_run_metadata`** pointers.” |
| **Lambda (container)** | “Same **transform code** as my laptop, packaged in **ECR**, triggered **on demand or on a schedule**.” |
| **EventBridge** | “**Scheduler** for periodic ingest; I use a **daily** rate in the repo JSON—**Uncertainty:** confirm **enabled** and **name** in console.” |
| **Glue** | “**Data Catalog**—databases **`jmi_*_v2`** map **S3 prefixes** to **table names**.” |
| **Athena** | “**Presto-style SQL** over those tables and **views**—no warehouse cluster.” |
| **QuickSight** | “**Athena datasets** feed **`dea final 9`**—**Uncertainty:** exact **visual ↔ dataset** map is **console-only**.” |

**One line to tie it:** “**S3 stores**, **Glue describes**, **Athena queries**, **QuickSight shows**.”

---

## 6. How to explain Bronze / Silver / Gold live

**Bronze:** “**Immutable** vendor JSON in a **stable envelope**—if Silver mapping changes, I can **replay**.”

**Silver:** “**Analytics grain = one job row**, typed fields, **`job_id`** dedupe, **`source=`** paths—**cleaning happens here**, not in BI.”

**Gold:** “**Monthly aggregates**: skills, roles, locations, companies, plus **`pipeline_run_summary`** and **`latest_run_metadata`**—**small files**, **partitioned by `posted_month` and `run_id`** so Athena **filters cheaply**.”

**Gesture:** Three folders in the bucket—**bronze → silver/jobs → gold/…**—same story as the code (`src/jmi/paths.py`).

---

## 7. How to explain Glue, Athena, and manual DDL live

“**Glue** does not move data; it **stores table definitions** pointing at **S3**. I use **DDL checked into the repo** (`infra/aws/athena/ddl_*.sql`, deploy scripts) so **my bucket paths and partition keys match** what the pipeline writes. **Athena** is just the **query engine** that reads those **external tables**.”

Add projection in one breath: “Gold tables use **partition projection** so I don’t **`MSCK REPAIR`** every new **`run_id`**—but **`run_id`** values must stay **consistent** with S3 (**enum** list / post-run sync—see Gold Lambda).”

---

## 8. How to explain why Glue Crawler was not used

“**Crawlers** infer schema and partitions by **scanning** objects—extra **cost** and **surprises** when vendors change JSON. I already **own** the **Hive-style paths** (`source=`, `posted_month=`, `run_id=`). **Manual DDL + projection** makes the catalog **predictable** and **versionable** with the repo. For this **MVP scale**, that’s simpler than fighting crawler drift.”

---

## 9. How to explain why Lambda was used

“Transforms are **short Python/pandas** jobs—**same entrypoints** locally and in the cloud. **Lambda with a container image** avoids running a **24/7 cluster**; I pay **per invocation**, chain **ingest → silver → gold** with **async invoke**, and stay inside **student-scale** data. If data grew huge, I’d shard or move to **Spark**—honest limit.”

---

## 10. How to explain why S3 layout matters

“Athena and Glue **don’t read ‘a table’**—they read **prefixes**. If **`source=`** or **`posted_month=`** is wrong, queries **miss files** or **scan everything**. Layout is the **contract** between **writers** (pipeline) and **readers** (DDL/views). That’s also why **cost** stays under control: **push filters** to **partitions**.”

---

## 11. How to explain the dashboard sections live (conceptual)

**Do not invent sheet names** if you are unsure—say: “My dashboard **`dea final 9`** is organized into **EU (Arbeitnow)**, **India (Adzuna)**, and **comparison** stories.”

Typical **story beats** (aligned with `jmi_analytics_v2` view families):

- **EU sheet:** KPI-style strips, **roles**, **locations**, **skills**, **companies**—fed by views like **`sheet1_kpis`**, **`role_pareto`**, **`location_top15_other`**, etc. (exact attach list: **`QUICKSIGHT_V2_DATASET_STRATEGY.md`**).  
- **India sheet:** Parallel **Adzuna** views (`*_adzuna`, geo helpers where deployed).  
- **Comparison:** **Strict-common** alignment, **benchmark** views, **HHI / mix** style metrics—**say** that **fair comparison** can **drop months** when one region lacks that **`posted_month`**.

**Say:** “Each visual traces to **Athena** → **`jmi_analytics_v2`** view → **`jmi_gold_v2`** facts → **S3 Parquet**.”

---

## 12. What order to follow when demoing

1. **Project overview** (30–60 s): outcome + medallion.  
2. **S3** (2–3 min): show **bronze / silver / gold** prefixes; one **sample path** with **`run_id`**.  
3. **Lambda + scheduler** (1–2 min): function purpose, **chain**, **schedule file** `rate(24 hours)` vs misleading name **`jmi-ingest-10min`**.  
4. **Glue + Athena** (2–4 min): open **databases**, run **one SELECT** with **`WHERE posted_month`** and **`source=`**.  
5. **QuickSight** (2–4 min): **`dea final 9`**, filter by **month/source**, show **comparison** if central to defense.

**Rule:** **Data path before pretty charts**—teachers trust **traceability**.

---

## 13. If I run the pipeline again in front of teacher—what to say at each stage

| Stage | Say (short) |
|-------|----------------|
| **Before start** | “I’m starting a **new batch**—this will get a fresh **`run_id`** and **`ingest_date`** stamped through Bronze.” |
| **Bronze / ingest** | “This writes **`raw.jsonl.gz`**—**full vendor JSON** in **`raw_payload`** for audit.” |
| **Silver** | “Here I **normalize**, extract **skills**, **dedupe** on **`job_id`**, write **Parquet** under **`silver/jobs/source=…`**.” |
| **Gold** | “Here I roll up to **`posted_month`** partitions and write **fact tables**—this is what **Athena filters** for dashboards.” |
| **Glue/Athena check** | “If this were the first time, I’d **extend projection** for new **`run_id`**—in steady state my Lambda **syncs** that; I’ll **verify with a `COUNT(*)`**.” |
| **QuickSight** | “Charts may need **SPICE refresh** if the dataset is not Direct Query—**Uncertainty:** your dataset mode.” |

---

## 14. Large viva Q&A (project-specific answers)

**Q: What is the business question?**  
**A:** “What skills, roles, locations, and employers show up **this month**, and how does **EU** compare to **India** on a **fair** month alignment—not mixing different calendar months.”

**Q: Why not query Bronze in QuickSight?**  
**A:** “**Bytes scanned** and **schema drift**—Bronze is for **replay and audit**; **Gold** is sized for **BI frequency**.”

**Q: What is `run_id`?**  
**A:** “A **batch id** from ingest—propagates to Silver/Gold paths so I can **reproduce** any chart for a **specific run**.”

**Q: Why `posted_month` not ingest month for Gold?**  
**A:** “The question is **when the job was posted**, not when I **fetched** it—otherwise March jobs ingested in April would **misstate** the market month.”

**Q: What is strict-common?**  
**A:** “Months where **both** sources have comparable **latest-per-month** rows—**inner join** on **`posted_month`**. It’s **fair** but can **exclude** a month if one side lacks coverage.”

**Q: Why views in `jmi_analytics_v2`?**  
**A:** “**Iterate KPI logic** without rewriting Parquet—BI semantics change faster than fact tables.”

**Q: How do you control cost?**  
**A:** “**Partition filters**, **Gold-first** queries, **reasonable schedule** (`rate(24 hours)` in repo), **avoid blind `s3 sync`**—see `cost_guardrails.md`.”

**Q: Single source of truth?**  
**A:** “**S3 objects**; Glue/Athena are **interfaces**; QuickSight is **presentation**.”

---

## 15. Trick / confusion questions and strong answers

| Trick question | Answer |
|----------------|--------|
| “Is Glue a database?” | “**No**—it’s **metadata**; data stays in **S3**.” |
| “Does Athena store data?” | “**No**—it **queries** S3 via the catalog.” |
| “Why is March missing?” | “Often **intersection/coverage**, not a ‘bug’—one source may lack that **`posted_month`** in the latest aligned window.” |
| “Why only April in comparison?” | “**Headline benchmark** may use **`strict_intersection_latest_month`**—**one** shared month. Use **monthly** comparison views for a **series**.” |
| “Is the crawler more automatic?” | “**More magic, less control**—I chose **explicit DDL** for this project’s **paths and projection**.” |

---

## 16. What limitations to admit honestly

- **Two public APIs**, not the global labor market—**sampling bias**.  
- **Rule-based skills**, not deep NLP—**noisy** edge cases.  
- **Geocoding** depends on **string quality** (India maps especially).  
- **Strict-common** comparison **drops** months—**honest** but not “complete history.”  
- **QuickSight wiring** for **`dea final 9`** is **not** fully reproducible from git—**Uncertainty**.  
- **Default scheduled ingest** path in repo/docs may emphasize **Arbeitnow**; **India** may be **manual or separate automation**—**Uncertainty:** state what **you** actually deployed.

---

## 17. Future improvements to mention

- **Automated projection updates** end-to-end tests in CI.  
- **Stronger monitoring**: CloudWatch alarms on Lambda failures, **empty Gold** checks.  
- **Richer geo** normalization for India.  
- **Optional Step Functions** if orchestration grows beyond async chains.  
- **Documented SPICE refresh** policy per dataset.

---

## 18. Key terms to remember

**Medallion**, **`raw_payload`**, **`job_id`**, **`posted_month`**, **`run_id`**, **`source=`**, **partition projection**, **`jmi_gold_v2` / `jmi_silver_v2` / `jmi_analytics_v2`**, **strict-common / intersection**, **`dea final 9`**, **SPICE vs Direct Query**, **EventBridge `rate(24 hours)`**.

---

## 19. Ten likely teacher questions

1. Why **Bronze** if you only chart **Gold**?  
2. How do you **deduplicate** jobs?  
3. Why **Parquet**?  
4. What breaks if **`run_id`** projection is stale?  
5. How is **EU vs India** comparison **fair**?  
6. Why **Lambda** instead of **Glue ETL**?  
7. Where is **PII** handled? (Public ads—still discuss **responsible use**.)  
8. How would you **validate** a chart number?  
9. What is your **biggest operational risk**? (Catalog drift, schedule, cost.)  
10. What would you do with **10× data**?

---

## 20. One strong final viva-ready summary paragraph

“I built **Job Market Intelligence** as an **AWS lakehouse**: **Bronze** preserves **raw API evidence**, **Silver** delivers **deduped job rows** with a **stable schema**, and **Gold** holds **monthly aggregates** keyed by **`posted_month`** and **`run_id`** so **Athena** can answer KPI questions **cheaply** with **partition filters**. **Glue** registers **versioned DDL** matching **S3 layout**—I avoided **crawlers** to keep **projection** and **paths** under control—and **Lambda** runs the **same Python transforms** serverlessly for **scale-to-zero** cost. **`jmi_analytics_v2`** encodes **BI semantics** (including **fair EU–India comparison** via **strict-common** months), and **QuickSight `dea final 9`** is my **capstone** presentation—**traceable** from dashboard **back to batch and posting month**, with **clear limits**: two vendors, **heuristic skills**, and **operational** catalog and **QuickSight** details that stay **verified in the console**, not guessed from the repo alone.”

---

## Infra uncertainty (not guessed)

- **IAM:** `infra/aws/iam/lambda-execution-policy.json` shows **S3 + Lambda invoke + logs**; **Gold handler** calls **`athena:StartQueryExecution`** (and **Glue** updates via Athena)—**live** execution role **must** include **Athena** permissions; **confirm** in **IAM console** for **your** account.
- **EventBridge:** Schedule JSON shows **`State": "ENABLED"`** and **`rate(24 hours)`** but **name** `jmi-ingest-10min` is **misleading**; **checkpoint docs** said schedule **may be disabled**—**live** state differs by environment.
- **QuickSight:** **Data source ARN**, **dataset IDs**, and **`dea final 9`** visual wiring—**console** only (`scripts/quicksight_create_datasets_v2.py` is a **template snapshot**).
- **Account/bucket:** **Hard-coded** IDs in `jmi-ingest-schedule.json`, DDL, workflows—**verify** against **your** AWS account.
