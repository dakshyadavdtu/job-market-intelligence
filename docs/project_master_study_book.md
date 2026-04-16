# Job Market Intelligence — Master Technical Study Book

**Audience:** You (deep internal manual). **Not** a marketing README.

**Grounding:** This document is built from **`src/jmi/`**, **`infra/aws/`**, **`scripts/`**, **`docs/`**, and Athena SQL in-repo. Where the repo does not contain live QuickSight wiring or account-specific state, that is **explicitly marked uncertain**.

---

## 1. What this project is (three levels)

### 1.1 Simplest possible words

This project pulls job postings from the internet, saves the raw answers safely, cleans them into one row per job, turns those rows into small monthly summary tables, and then uses cloud SQL and a dashboard so you can see skills, roles, places, and employers—without opening JSON by hand.

### 1.2 Slightly more technical

**Job Market Intelligence (JMI)** implements a **medallion data lake** on **Amazon S3**: **Bronze** (immutable gzipped JSON lines), **Silver** (deduplicated Parquet job rows with a strict schema), **Gold** (partitioned monthly aggregates). **AWS Glue Data Catalog** registers S3 paths as **tables**; **Amazon Athena** runs SQL (including **views** for BI semantics); **Amazon QuickSight** consumes Athena datasets. Orchestration uses **AWS Lambda** (chained async invokes) and **EventBridge Scheduler** for periodic runs. **Partition projection** in Glue avoids `MSCK REPAIR` for new `run_id` folders.

### 1.3 What the final deliverable actually is

1. **Reproducible pipeline code** (local + Lambda) that produces **versioned outputs** keyed by **`run_id`** and **`posted_month`**.  
2. **S3 objects** under predictable prefixes (`source=`, `posted_month=`, `run_id=`).  
3. **Glue/Athena metadata** (`jmi_gold_v2`, `jmi_silver_v2`, `jmi_analytics_v2`) with **manual DDL/deploy scripts**.  
4. **QuickSight assets** (in your AWS account) pointing at Athena—**not fully exported in git**.

---

## 2. Why this project exists (deep problem statement)

### 2.1 What is wrong with “raw job data”

- **Duplicates:** Same posting re-fetched across runs/pages → naive counts **double-count**.  
- **Changing APIs:** Field names and shapes drift; if you only keep “clean tables,” you **lose evidence** of what changed.  
- **Inconsistent tags:** “React”, “reactjs”, “REACT” in skills/tags.  
- **Messy locations:** Free-text city/state/country strings; maps break if you don’t normalize.  
- **Repeated pulls:** Micro-batch ingestion means **many runs**; without **`run_id`** lineage you cannot reconcile “which batch produced this KPI?”

### 2.2 Why a CSV export or direct dashboard is insufficient

A one-off CSV **freezes** semantics without **replay**; a dashboard on raw JSON **re-parses** everything forever (slow, expensive, non-reproducible). This project separates **evidence** (Bronze), **trustworthy rows** (Silver), and **fast answers** (Gold).

### 2.3 Why a pipeline + architecture is required

You need: **auditability**, **dedupe rules**, **month semantics** tied to posting time, **multi-source isolation** (`source=arbeitnow` vs `source=adzuna_in`), and **cheap queries** at BI time—hence layers, partitions, and aggregate tables.

**Teacher may ask:** *“Why not query the API in the dashboard?”*  
**Answer:** You’d pay latency and cost per view, lose history when the vendor drops postings, and you couldn’t prove numbers later. The lake preserves **batch truth**.

---

## 3. Full system story: zero → dashboard (narrative)

### 3.1 Source API → ingestion code

- **Arbeitnow:** `src/jmi/connectors/arbeitnow.py` paginates `https://www.arbeitnow.com/api/job-board-api`, wraps each job via `to_bronze_record`.  
- **Adzuna India:** separate ingest module; same **Bronze envelope** pattern, different payload mapping.  
- **Incremental state:** `src/jmi/pipelines/bronze_incremental.py` + connector state under `state/` (when used) controls fetch watermarks / filtering strategies.

**Design decision:** Ingestion is **faithful**—business cleaning is **not** done here.

### 3.2 Bronze

- **Input:** Python dicts from API.  
- **Logic:** `ingest_live.run()` assigns **`run_id = new_run_id()`**, **`bronze_ingest_date`** (UTC date), writes **`raw.jsonl.gz`** via `write_jsonl_gz` to `bronze_raw_gz(cfg, ingest_date, run_id)`.  
- **Output:** `manifest.json`, `health/latest_ingest*.json`.  
- **AWS:** If `JMI_DATA_ROOT=s3://bucket/prefix`, writes go to S3 (same paths).  
- **Why exists:** Legal/technical **audit** and **replay** if Silver rules change.

### 3.3 Silver

- **Input:** Bronze JSONL.gz path (local or S3).  
- **Logic (`transform_silver.run`):**  
  - Parse lineage from path (`run_id`, `ingest_date`).  
  - Flatten `raw_payload` per row; **`extract_silver_skills`**; normalize title/company/location (`silver_schema.py`).  
  - Dedupe on **`job_id`**.  
  - Merge with prior silver history (`_merge_with_prior_silver` / `load_silver_jobs_history_union`) → **`merged/latest.parquet`**.  
  - **`run_silver_checks`** must PASS or pipeline raises.  
- **Output:** batch Parquet + merged Parquet + `quality/silver_quality_*.json`.  
- **Why exists:** Single **contract** for Gold/Athena; deduped **job grain**.

### 3.4 Gold

- **Input:** merged Silver (or union strategy—see `transform_gold._resolve_silver_dataframe`).  
- **Logic:** `assign_posted_month_and_time_axis` → filter valid `posted_month` → loop months (optionally incremental months for live) → write fact Parquet via `gold_fact_partition(cfg, table, posted_month=..., pipeline_run_id=...)`.  
- **Also writes:** `gold_latest_run_metadata_file(cfg)` single-row pointer Parquet for that **source**.  
- **Output:** five fact families + `pipeline_run_summary` rows + quality JSON.  
- **Why exists:** **Small** files for Athena/QS; predictable cost.

### 3.5 Glue / Athena tables

- **Glue:** External tables with `LOCATION s3://…/gold/<table>/` and **TBLPROPERTIES** for **partition projection** (`ddl_gold_*.sql`).  
- **Deploy:** `scripts/deploy_athena_v2.py` rewrites `jmi_gold.` → `jmi_gold_v2.` and runs `CREATE` statements.

### 3.6 Athena views

- **`jmi_analytics_v2`:** Views for KPI slices, EU/India helpers, **comparison** (`docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql` + deploy scripts).  
- **Why:** Encode “latest month intersection”, “HHI”, “aligned top-20” **without** new Parquet for every tweak.

### 3.7 QuickSight

- Datasets reference **database + view/table** (or custom SQL). SPICE caches query results.  
- **Uncertainty:** Exact dataset IDs for **`dea final 9`** are **only in QuickSight**—verify in console before claiming in viva.

### 3.8 Teacher-facing dashboard

- Analytical story: **Europe** (Arbeitnow), **India** (Adzuna), **Comparison** (strict-common / HHI / mix views)—implemented primarily through **`jmi_analytics_v2`** views + Gold facts.

---

## 4. Full AWS infrastructure map (project-specific)

| Service | Role in *this* repo | Repo touchpoints | Alternatives not chosen | Why this choice |
|--------|---------------------|------------------|-------------------------|-----------------|
| **S3** | Lake storage + Athena results | `DataPath`, `paths.py`, `JMI_DATA_ROOT` | Local disk only | Cloud demo + shared data; Parquet scan from Athena |
| **Lambda** | Run `ingest_live` → async Silver → async Gold | `infra/aws/lambda/handlers/*.py` | EC2 cron, Glue ETL | Pay per invoke; same Python as local; no cluster ops |
| **ECR + container image** | Lambda **PackageType Image** (not zip) | `infra/aws/lambda/Dockerfile`, `deploy_ecr_create_update.sh` | Zip deployment | Dependencies + Linux parity; repo documents image path |
| **EventBridge Scheduler** | Trigger ingest on schedule | `infra/aws/eventbridge/jmi-ingest-schedule.json` | CloudWatch Events legacy | Scheduler API / rate expressions |
| **Glue Data Catalog** | Tables + partitions metadata | DDL under `infra/aws/athena/ddl_*.sql` | Hive metastore on EC2 | Managed, Athena-native |
| **Athena** | SQL + `ALTER TABLE SET TBLPROPERTIES` | `deploy_athena_v2.py`, `athena_projection.py` | Presto on EMR, Redshift | Serverless; fits student budget |
| **QuickSight** | Dashboards | `docs/dashboard_implementation/*` | Grafana, Superset | AWS-native SPICE integration |
| **IAM** | Lambda invoke rights, S3 access | `infra/aws/iam/` | Overly broad admin keys | Least privilege (intent in repo) |
| **CloudWatch Logs** | Lambda stdout/stderr | (implicit) | X-Ray (optional) | Basic ops visibility |
| **Billing / Budgets / Anomaly** | Cost guardrails | `docs/cost_guardrails.md` | Ignored spend | Student cap mindset |

### 4.1 Services explicitly **not** core (and why)

| Service | Why teams use it | Why **this** project avoided it |
|---------|------------------|----------------------------------|
| **Glue Crawler** | Auto-discover partitions/schema | Conflicts with **partition projection** + **manual `run_id` enum**; adds **S3 LIST** cost |
| **Glue ETL / Spark** | Big distributed transforms | Transform logic is **pandas** in Python; data volume fits Lambda + batch |
| **Step Functions** | Orchestrate Lambdas | **Async invoke chain** is enough; fewer moving parts |
| **EC2** | Always-on jobs | Violates cost posture; ops burden |
| **RDS / Redshift** | OLTP/warehouse | Lake + Athena chosen for “query S3 directly” |
| **EMR** | Big data | Not needed at current scale |

**Teacher may ask:** *“Is Lambda ‘enough’?”*  
**Answer:** For **micro-batch** Parquet writes and pandas transforms, yes. Limit is **timeout** and **memory** if datasets explode—then you’d shard or move to Spark.

---

## 5. Why each AWS component exists (service-by-service deep dive)

### 5.1 S3

- **Why needed:** Durable, cheap **object store** for immutable Bronze and columnar Silver/Gold.  
- **Why not local only:** Teacher demo / shared bucket / Athena integration.  
- **Why not DB-first:** You’d duplicate blobs or lose file-level replay; lake pattern keeps **one copy** of truth in open formats.  
- **Why prefixes matter:** Athena + Hive use **`/` keys** as logical partitions; wrong layout ⇒ wrong scans or missing rows.

### 5.2 Lambda (three stages)

Functions (names from `infra/aws/lambda/README.md`): **`jmi-ingest-live`**, **`jmi-transform-silver`**, **`jmi-transform-gold`**.

**Chain:**

1. **Ingest handler** (`ingest_handler.py`): runs `ingest_live.run()`; if `invoke_silver` and records > 0, async-invokes Silver with `bronze_file=result["bronze_data_file"]`, `run_id=result["run_id"]`.  
2. **Silver handler** (`silver_handler.py`): runs `transform_silver.run(bronze_file=...)`; async-invokes Gold with `silver_file`, `merged_silver_file`, `run_id`.  
3. **Gold handler** (`gold_handler.py`): sets incremental month env unless overridden; runs `transform_gold.run(...)`; then **`sync_gold_run_id_projection_from_s3()`** which **lists S3** under `gold/role_demand_monthly/` to extract all `run_id=` segments and runs **`ALTER TABLE ... SET TBLPROPERTIES ('projection.run_id.values'=...)`** on all five fact tables.

**Why Lambda vs Glue ETL:** Transform code is **your Python** (`src/jmi/pipelines/*`), not Spark. **Teacher narrative:** “Same code runs locally and in Lambda; cloud adds schedule and scale-to-zero.”

**Limitations:** **15-minute** timeout (configurable), package size, **cold start**. **Not used for Adzuna on schedule** in default design—README states scheduled ingest is **Arbeitnow** only unless you change handler.

### 5.3 Glue Data Catalog

- **What Glue does here:** Stores **table definitions** (columns, serde, `LOCATION`, **TBLPROPERTIES** for projection).  
- **What Glue does *not* do:** Run Spark jobs (not used), crawl (not used as primary).  
- **Manual work replacing crawler:** `CREATE EXTERNAL TABLE` SQL files + **`ALTER TABLE` for `projection.run_id.values`** + deploy scripts.

### 5.4 Athena

- **Why:** SQL interface; integrates with Glue; pay per query.  
- **Why views:** Encode latest-run and comparison logic once; multiple QS datasets can share a view.  
- **Between Gold and QS:** QS dataset → (Athena) → views/tables → S3 reads.

### 5.5 QuickSight

- **Binding:** Dataset stores **data source + database + table/view** (or SQL text). Renaming/dropping Athena objects **breaks refresh** until updated.  
- **Imported vs used:** Inventory docs warn duplicates may exist—**console** is source of truth.

### 5.6 EventBridge Scheduler

- **Repo file:** `rate(24 hours)` in `jmi-ingest-schedule.json` (name may still say `10min`—**misleading**; verify live schedule).  
- **Cost angle:** More frequent runs ⇒ more Lambda + more S3 writes + **more projection sync LIST traffic**—ties to request-cost incidents.  
- **Teacher may ask:** *“Why not hourly?”* **Answer:** Cost cap + API politeness + MVP; can tune if budget allows.

### 5.7 IAM / CloudWatch / Billing / CloudShell

- **IAM:** Roles for EventBridge→Lambda, Lambda→S3, Lambda→Lambda invoke.  
- **CloudWatch Logs:** Proof Lambda ran; grep `run_id`.  
- **Billing / Anomaly:** Use Cost Explorer; **APS3-Requests-Tier1** is a **billing line** for S3 request charges in **ap-south-1** (naming may vary slightly by invoice format)—high numbers ⇒ LIST/GET patterns.  
- **CloudShell:** Convenience CLI; **cannot build Docker images** (per Lambda README).

---

## 6. Repo structure walkthrough (guided tour)

| Path | What it is | When to mention in viva |
|------|------------|-------------------------|
| `src/jmi/config.py` | `AppConfig`, `DataPath` (local or `s3://`), `new_run_id()` | Core config |
| `src/jmi/paths.py` | Canonical S3 key layout | **Must know** for partitions |
| `src/jmi/connectors/arbeitnow.py` | Fetch, hash `job_id`, bronze record | Source + identity |
| `src/jmi/connectors/adzuna.py` | India source | Multi-source |
| `src/jmi/connectors/skill_extract.py` | Allowlist/rules | Skills chapter |
| `src/jmi/pipelines/ingest_live.py` | Bronze writer | Live demo start |
| `src/jmi/pipelines/transform_silver.py` | Silver | Dedup + contract |
| `src/jmi/pipelines/transform_gold.py` | Gold aggregates | posted_month |
| `src/jmi/pipelines/gold_time.py` | `posted_month` vs fallback | Time semantics |
| `src/jmi/pipelines/silver_schema.py` | Normalization helpers | Cleaning depth |
| `src/jmi/utils/quality.py` | Silver checks | Quality gates |
| `src/jmi/aws/athena_projection.py` | **LIST S3** + **ALTER TABLE** projection | Glue sync mechanics |
| `infra/aws/lambda/handlers/` | Lambda entrypoints | AWS story |
| `infra/aws/athena/ddl_gold_*.sql` | Fact tables + projection | Manual DDL proof |
| `infra/aws/athena/analytics_v2_*.sql` | EU/India helper views | BI layer |
| `docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql` | Comparison | EU vs India |
| `scripts/deploy_athena_v2.py` | Gold DB deploy orchestrator | “How metadata is deployed” |
| `scripts/pipeline_live_sync.py` | Local run + `aws s3 sync` + projection update | Demo / ops |
| `dashboard/app.py` | Streamlit local QA | Not primary QS |
| `infra/aws/athena/archive_non_v2_ddl/` | Archived Glue DDL after catalog cleanup | History |

---

## 7. Source strategy (deep)

- **Designed for:** Multiple **`source=`** partitions sharing one Silver schema.  
- **Implemented:** `arbeitnow`, `adzuna_in` end-to-end in code.  
- **Arbeitnow:** EU-focused public API; default Lambda ingest.  
- **Adzuna:** India postings; enables **comparison** views joining both sources on **`posted_month`**.  
- **Comparison meaning:** Not “merge into one country”—**side-by-side** analytics with **intersection** months for fairness.  
- **Historical CSV:** Documented future hook—**not** production path in code reviewed here.

**Why `source=` partitions matter:**  
Physically isolates keys; Glue **`projection.source.values`** can prune; prevents accidental mixing in Athena paths.

---

## 8. Bronze layer — extremely deep

### 8.1 General meaning

Bronze is the **immutable-ish raw layer**: you optimize for **audit**, not pretty columns.

### 8.2 JMI Bronze specifics

- **Envelope fields:** `source`, `schema_version`, `job_id`, `job_id_strategy`, `ingested_at`, **`raw_payload`**, batch fields `run_id`, `bronze_ingest_date`, `batch_created_at`.  
- **`raw_payload` untouched** (JSON as returned / embedded). Salary, long description, URL typically stay here if not in Silver contract (`docs/data_dictionary.md`).

### 8.3 Paths

`bronze/source=<slug>/ingest_date=YYYY-MM-DD/run_id=<id>/raw.jsonl.gz`  
Optional Arbeitnow slice: `source=arbeitnow/slice=<tag>/...` when `JMI_ARBEITNOW_SLICE` set.

### 8.4 Why `ingest_date` and `run_id`

- **`ingest_date`:** Partition by **when you pulled** (UTC date). Helps ops (“today’s batch”).  
- **`run_id`:** Unique batch id—**lineage** through Silver/Gold.

### 8.5 Manifests and health

- **`manifest.json`:** Counts, paths, incremental diagnostics.  
- **`health/latest_ingest.json`:** Small pointer for humans/automation.

### 8.6 Teacher questions

- *“Why not clean in Bronze?”* Cleaning would destroy vendor truth; you couldn’t re-derive if rules change.  
- *“What if we skip Bronze?”* You lose audit and replay—**not acceptable** for a data engineering defense.

---

## 9. Silver layer — extremely deep

### 9.1 Why Silver exists

Convert semi-structured JSON into **typed, deduplicated job rows** with a **strict contract** (`project_silver_to_contract`).

### 9.2 Transform sequence (mechanics)

1. Read Bronze JSON lines.  
2. Enforce **single source** per file.  
3. For each row: extract display fields from `raw_payload`, compute **`extract_silver_skills`**, normalize title/company/location, `posted_at` from payload rules.  
4. Dedupe **`job_id`** within batch.  
5. Quality checks (**titles, dup keys**, etc.).  
6. Merge with historical Silver (`_merge_with_prior_silver`) sorted by `(bronze_ingest_date, bronze_run_id, ingested_at)` then dedupe **`job_id` keep last**.  
7. Write **batch Parquet** + **merged/latest.parquet**.

### 9.3 Deterministic `job_id`

Hash strategy in connectors ensures same logical job → same id → **stable dedupe** across runs.

### 9.4 Lineage fields

`bronze_run_id`, `bronze_ingest_date`, `bronze_data_file` propagate to Silver (and into Gold rows) for **traceability**.

### 9.5 Layout evolution

- **Canonical active:** `silver/jobs/source=<slug>/ingest_date=…/run_id=…/part-00001.parquet`.  
- **Legacy problem:** Flat `silver/jobs/ingest_date=…` without `source=` mixed operational story with Adzuna’s modular layout; also legacy Glue **`jmi_silver.jobs`** could write flat Hive partitions **under** `silver/jobs/` (catalog cleanup archived in `archive_non_v2_ddl`).  
- **`silver_legacy/`:** Optional archive for old flat batches—keeps confusion out of active `silver/jobs/`.

### 9.6 Helper views from Silver

`jmi_silver_v2` may expose merged jobs (`ddl_silver_v2_*`)—EU/India **foundation** views in `analytics_v2_*_silver_foundation.sql` read Silver for map/sankey helpers.

### 9.7 Teacher questions

- *“What if Silver didn’t exist?”* You’d aggregate JSON in Athena—**slow, expensive, inconsistent dedupe**.

---

## 10. Gold layer — extremely deep

### 10.1 Why Gold exists

**Pre-aggregated** small Parquet files for BI—**filter partitions**, not scan all jobs.

### 10.2 `posted_month` semantics (`gold_time.py`)

- Primary: calendar month of **`posted_at`** (UTC parsing with epoch fallbacks).  
- Fallback: month of **`bronze_ingest_date`** when `posted_at` bad—**`time_axis`** records `'ingest_fallback'`.  
- **Why not naive ingest month only:** Business question is usually “jobs **posted** in March,” not “jobs **ingested** in March.”

### 10.3 Paths

`gold/<table>/source=<slug>/posted_month=YYYY-MM/run_id=<pipeline_run_id>/part-00001.parquet`  
(`gold_root_effective` adds `gold/slice=<tag>/` for Arbeitnow slice runs.)

**Note:** Older docs/README lines may still say `ingest_month`—**Glue DDL and code use `posted_month`** for v2 facts.

### 10.4 `run_id` in Gold

Gold **`run_id` partition** is the **pipeline run id** (`transform_gold` generates `pipeline_run_id`), not necessarily identical to every row’s `bronze_run_id`—rows still carry **`bronze_run_id`** in the Parquet body for lineage.

### 10.5 Fact families (row meaning)

| Table | Row meaning | Supports |
|-------|-------------|----------|
| `skill_demand_monthly` | **Distinct-job** counts aggregated per skill tag within month/source/run | Skill charts, HHI (note tag mass caveats in comparison SQL) |
| `role_demand_monthly` | Jobs per normalized **role/title** grain | Role pareto, comparison totals |
| `location_demand_monthly` | Jobs per normalized **location** label | Maps, top-N |
| `company_hiring_monthly` | Jobs per normalized **company** | Treemap |
| `pipeline_run_summary` | Summary stats for validation | “Did pipeline hang together?” |
| `latest_run_metadata_arbeitnow` / `_adzuna` | **Single-row** pointer Parquet at `gold/source=<slug>/latest_run_metadata/` | “Latest run” filters |

**Deploy naming:** SQL files may say `CREATE ... jmi_gold.latest_run_metadata_arbeitnow` but `deploy_athena_v2.py` patches to **`jmi_gold_v2.*`**.

---

## 11. S3 path architecture and partition strategy (detailed)

### 11.1 Active patterns (examples)

- Bronze: `s3://bucket/bronze/source=arbeitnow/ingest_date=2026-04-14/run_id=20260414T120000Z-abc/raw.jsonl.gz`  
- Silver: `.../silver/jobs/source=arbeitnow/ingest_date=.../run_id=.../part-00001.parquet`  
- Gold: `.../gold/role_demand_monthly/source=arbeitnow/posted_month=2026-03/run_id=20260412T155712Z-e2e07b3f/part-00001.parquet`  
- Metadata: `.../gold/source=arbeitnow/latest_run_metadata/part-00001.parquet`

### 11.2 Why each partition key

| Key | Purpose |
|-----|---------|
| `source=` | Multi-source isolation + Glue enum projection |
| `ingest_date=` (Bronze) | Batch/day ops |
| `run_id=` | Lineage / reproducibility |
| `posted_month=` (Gold) | Business month analytics |

### 11.3 Legacy / archive

- **`gold_legacy/`**, **`gold/comparison_*` legacy prefixes** mentioned in `paths.py` comments—**not** written by current modular Gold writer.  
- **Catalog:** Non-v2 DBs dropped; see `archive_non_v2_ddl/`.

### 11.4 How wrong layout breaks Athena

- If **`projection.run_id.values`** missing new `run_id` folders, Athena returns **0 rows** even though S3 has objects.  
- If **`storage.location.template`** stuck in Glue (historical issue), paths may not match—see `docs/aws_live_fix_gold_projection.md`.

---

## 12. Detailed end-to-end execution logic (live narration script)

### 12.1 Local run (commands)

Typical EU path:

```bash
python -m src.jmi.pipelines.ingest_live
python -m src.jmi.pipelines.transform_silver
python -m src.jmi.pipelines.transform_gold
```

Adzuna: use `ingest_adzuna` / `--source adzuna_in` per runbooks.

**Say while running:**  
1. “Ingest creates a new **`run_id`** and writes **Bronze** gzip.”  
2. “Silver reads that Bronze file path, dedupes **`job_id`**, merges history, writes **Parquet**.”  
3. “Gold assigns **`posted_month`**, writes **monthly aggregates** and **latest_run_metadata** for this source.”

### 12.2 Verify outputs

- Bronze: `manifest.json`, `raw.jsonl.gz`  
- Silver: `silver_quality_*.json`, batch + merged parquet paths in JSON  
- Gold: `gold_quality_*.json`, fact paths under `gold/.../posted_month=.../run_id=.../`

### 12.3 AWS run

EventBridge → **ingest Lambda** → **silver Lambda** → **gold Lambda** → **S3 writes** → **Gold handler lists S3** to update **projection enums** → Athena can see new partitions.

**Failure points:** Empty Bronze, Silver checks fail, Gold no valid `posted_month`, **IAM** missing `s3:ListBucket` for projection sync, Athena **`run_id` not in enum**.

---

## 13. Each Lambda explained separately

### 13.1 `ingest_handler`

- **Input event:** e.g. `{"trigger":"eventbridge"}`.  
- **Logic:** `ingest_live.run()`.  
- **Output:** API Gateway-style JSON; side effect: async **Silver** invoke if records > 0.  
- **Writes:** Bronze keys, manifest, health, state.  
- **Env:** `JMI_SILVER_FUNCTION_NAME`.

### 13.2 `silver_handler`

- **Input:** `bronze_file`, `run_id`.  
- **Logic:** `transform_silver.run(bronze_file=...)`.  
- **Output:** JSON with `output_file`, `merged_silver_file`.  
- **Triggers:** Async **Gold** with silver paths.  
- **Env:** `JMI_GOLD_FUNCTION_NAME`.

### 13.3 `gold_handler`

- **Input:** `silver_file`, `merged_silver_file`, `run_id`, optional `source_name`, incremental flags.  
- **Logic:** `gold_run(...)` then **`sync_gold_run_id_projection_from_s3()`**.  
- **Touches:** S3 read/list, Athena **`ALTER TABLE`**.  
- **Why separate:** Gold is slower + needs **Glue update**; separation isolates failures.

**Viva line:** “Three Lambdas keep concerns separate and allow independent retries/logging.”

---

## 14. Manual DDL / Glue / Athena metadata (deep)

### 14.1 What “manual” means

- **Author SQL files** in git (`ddl_gold_*.sql`).  
- **Run deploy scripts** (`deploy_athena_v2.py`) which:  
  - `CREATE DATABASE jmi_gold_v2`  
  - `CREATE EXTERNAL TABLE ... TBLPROPERTIES ('projection.enabled'='true', ...)`  
  - Run subprocesses for **analytics** + **comparison** view scripts.

### 14.2 What a crawler would have done

- **LIST** S3, infer columns, **add partitions**—but would **not** maintain **`projection.run_id.values`** as a curated enum aligned with your pipeline.

### 14.3 Partition projection essentials

- **`projection.posted_month.type=date`** + range → month templates.  
- **`projection.run_id.type=enum`** + **`values`** → must include **every** run id directory you need to query.  
- **Update mechanisms:**  
  - **deploy_athena_v2.RUN_ID_ENUM** (static in script) for initial deploy;  
  - **Lambda `sync_gold_run_id_projection_from_s3`** (dynamic from S3 LIST);  
  - **`pipeline_live_sync.update_gold_v2_run_id_projection`** after local sync.

### 14.4 Risks if you used crawler instead

- Schema drift on inferred types  
- Accidental table overwrite  
- Harder **reproducible** infra-as-code review

### 14.5 Non-v2 database cleanup

- **`jmi_gold`, `jmi_silver`, `jmi_analytics`** metadata removed; definitions archived—**S3 not deleted**.

**Teacher Q:** *“Manual DDL sounds error-prone?”*  
**A:** “We version it in git, match the pipeline’s exact paths, and avoid crawler drift; trade-off is operational discipline.”

---

## 15. Skill extraction and cleaning logic (deep)

**Implementation:** `skill_extract.py` — `SKILL_ALLOWLIST`, aliases, stoplist, phrase matching, context from title/description; Adzuna may call **`adzuna_enrich_weak_skills`** after base extraction.

**Why generic words hurt:** Inflate demand for nonsense “skills.”

**Not NLP:** No embeddings; **transparent rules**—good for MVP explainability.

**Downstream impacts:**

- **Skill demand monthly:** driven by extracted tags.  
- **HHI on tags:** measures concentration of **tag mass**, not unique-job skill independence—comparison SQL header is honest about this.

---

## 16. Other cleaning / normalization

- **Titles:** `normalize_title_norm` strips gender parentheses clutter (DE), punctuation.  
- **Companies:** `normalize_company_norm` lowercases, removes pipes, drops leading “The ” in long strings.  
- **Locations:** `normalize_location_raw` + extensive India parsing in `silver_schema.py` (state lists, city aliases).  
- **Remote:** `remote_type_for_silver`—Arbeitnow has signal; Adzuna may be `unknown`.  
- **Views removed/kept:** `deploy_athena_v2.py` lists **obsolete** views dropped at deploy end—cleanup to reduce duplicate QuickSight confusion.

---

## 17. Athena databases and design separation

| DB | Holds | Why separate |
|----|--------|--------------|
| `jmi_gold_v2` | **Physical** external tables over Gold Parquet | Facts + pointers |
| `jmi_silver_v2` | Silver merged / jobs | Job-grain QA / helpers |
| `jmi_analytics_v2` | **Views** mostly | Presentation logic; can iterate without rewriting Parquet |

**Why not collapse views into `jmi_gold_v2` for “neatness”:**  
QuickSight datasets bind to **database+view**; moving breaks unless you repoint—**operational risk** for little gain. Separation is **standard** (facts vs semantic layer).

---

## 18. View dependency logic (deep)

- Views can reference **other views** and **base tables**. Example pattern: `latest_pipeline_run` style views (where still present) read metadata pointers; comparison views read **`jmi_gold_v2.role_demand_monthly`** and **`skill_demand_monthly`**.  
- A view **not on the dashboard** may still be a **CTE dependency** in another view—dropping requires **order** and dependency checks.  
- **deploy_athena_v2** “obsolete drops” list is a curated **prune** to match minimal set.

---

## 19. Comparison layer — very deep

### 19.1 Purpose

Compare **EU (Arbeitnow)** vs **India (Adzuna)** on aligned months with explicit **alignment rules**.

### 19.2 Strict-common vs latest-month semantics (from SQL)

- **`month_bounds`:** typically **previous calendar month through current month** window based on `current_timestamp` (see `ATHENA_VIEWS_COMPARISON_V2.sql`).  
- **`month_latest_eu` / `month_latest_ad`:** per `posted_month`, choose **`MAX(run_id)`** from `role_demand_monthly`—a pragmatic “latest successful gold run for that month.”  
- **`intersection`:** months present in **both** sides.  
- **`strict_intersection_latest_month`:** **max** month in intersection—comparison “latest aligned” month can **exclude** a month that exists on only one side (explains **March missing** style issues when India/EU coverage differs).  
- **HHI:** On **skill tag shares** within month/run/source; `month_in_strict_intersection` flag labels rows.

### 19.3 Why view-heavy

Business rules changed during project; views iterate faster than rewriting **derived_*** physical tables (some **`derived_`** Glue tables explicitly dropped in `comparison_v2_views.sql`).

### 19.4 Dashboard attachment (**uncertain**)

Exact which comparison visuals sit on **`dea final 9`** must be confirmed in **QuickSight**—repo provides **candidate** views (`comparison_*`, `v2_*`).

---

## 20. QuickSight layer (depth + uncertainty)

- **`dea final 9`:** Name appears in user requirements; **no JSON export in repo**—treat dataset↔visual mapping as **verify in console**.  
- **Sections (intent from docs):** EU Arbeitnow KPIs, India Adzuna geo/skill visuals, comparison charts.  
- **SPICE vs DQ:** Checklist recommends SPICE for stable demos; DQ for dev.

---

## 21. Visual strategy chapter

For each visual type, **what it shows in JMI**, **why chosen**, **pitfall**:

| Visual | Use | Pitfall |
|--------|-----|---------|
| KPI cards | Headline totals/shares | Wrong `run_id` filter |
| Heat map | India state × skill density | Bad geo fields |
| Radar | Profile shape | Needs stable axes |
| Sankey | Flows (EU helpers) | Easy to misread without labels |
| Treemap | Concentration | Treemap vs bar duplication |
| Donut/pie | Skill mix | Not additive across jobs if tags overlap |
| Scatter | City metrics | Outliers dominate |
| Histogram | Employer size | Bin choice matters |
| Box plot | Skill per job distribution | Needs job-grain helper |
| Line | Month trends | Window vs intersection |
| Clustered column | EU vs IN totals | Alignment semantics |
| HHI | Concentration | Definition is tag-mass based |

---

## 22. Cost architecture and anomaly (deep)

- **Why serverless:** No idle clusters; pay per invoke/query/storage.  
- **APS3-Requests-Tier1:** Billing category for **S3 request charges** in Mumbai region—spikes when LIST/GET explode.  
- **Project behaviors that increase requests:**  
  - **`sync_gold_run_id_projection_from_s3`** lists `gold/role_demand_monthly/` (necessary but LIST-heavy on large buckets)  
  - **`aws s3 sync`** without excludes  
  - Frequent schedules  
- **Mitigation:** Reduce schedule, sync excludes (`pipeline_live_sync` documents silver excludes), avoid redundant listing.

---

## 23. Major issues / bugs / fixes (project history)

| Issue | Cause | Fix / lesson |
|-------|-------|--------------|
| March “missing” in comparison | **Intersection** dropped months without dual coverage | Explain coverage; adjust window; refresh both sources |
| Athena 0 rows despite S3 data | Glue **`storage.location.template`** / stale projection | `aws_live_fix_gold_projection.md` |
| Silver flat layout confusion | Legacy Hive + mixed modular paths | Source-prefixed active layout; legacy archive |
| Strict-common looks “April-only” | `strict_intersection_latest_month` + rolling window | Data + SQL semantics, not just UI |
| Duplicate analytics views | Iterative QS experiments | Deploy script drops obsolete views |
| Cost anomaly | S3 requests | Schedule + sync discipline |
| Non-v2 Glue DBs | Retired architecture | Metadata dropped; DDL archived |

---

## 24. What to say while running the pipeline live (script)

1. “I’ll run **ingest**—this only captures raw vendor JSON into **Bronze** with a new **`run_id`**.”  
2. “**Silver** flattens `raw_payload`, applies **dedupe** and **skill rules**, writes **Parquet**.”  
3. “**Gold** computes **`posted_month`** from `posted_at` and writes **monthly aggregates**.”  
4. “In AWS, **Lambda** runs the same code; **Gold** updates **partition projection** so Athena sees the **`run_id`** folder.”  
5. Open **S3** to show paths; **Athena** to `SELECT COUNT(*)` with **`posted_month` and `run_id` filters**; **Glue** table properties for projection.

**Mid-run teacher questions:** Be ready to show **`manifest.json`**, **`silver_quality`**, **`gold_quality`**, and one **`EXPLAIN`** or `COUNT(*)` query plan awareness (partition pruning).

---

## 25. Viva questions — expanded answer bank (project-grounded)

Each item is phrased as **question → strong answer you can say aloud**.

### 25.1 Motivation & scope

**Q: Why this project?**  
**A:** Job postings are messy, duplicated, and API-driven. We need **reproducible KPIs** (skills, roles, locations, employers) with **lineage** (`run_id`) so results can be defended in placement / mentoring contexts—not a one-off scrape.

**Q: Why these sources (Arbeitnow + Adzuna)?**  
**A:** **Arbeitnow** gives a public EU board API without proprietary contracts—good for demo. **Adzuna India** adds a second geography and forces a real **multi-source** lake design (`source=` partitions) and a **comparison** story EU vs India.

**Q: Is this the whole job market?**  
**A:** No—two public feeds. The project proves **engineering** (lake + lineage + BI), not global labor economics coverage.

### 25.2 AWS & architecture choices

**Q: Why AWS at all?**  
**A:** **S3 + serverless query + managed BI** matches a small budget: pay for storage and queries, no always-on cluster. Same code runs locally for dev.

**Q: Why S3 not Postgres first?**  
**A:** We need **cheap immutable storage** for raw JSONL and columnar Parquet at scale; Athena/Glue integrate directly with **open files**—easier replay and audit than loading everything into a relational DB first.

**Q: Why Lambda?**  
**A:** The transforms are **short Python/pandas** jobs triggered on a schedule or chain. Lambda matches **micro-batch** without maintaining EC2. **Trade-off:** timeout/memory limits—acceptable at this data size.

**Q: Why not AWS Glue ETL (Spark)?**  
**A:** No Spark dependency in `src/jmi/pipelines`; complexity and cost aren’t justified. If data grew 100×, we’d consider Spark or batch jobs.

**Q: Why not Step Functions?**  
**A:** Ingest → Silver → Gold is a **simple chain**; async Lambda invoke is fewer moving parts. Step Functions would add state-machine ops overhead for MVP.

**Q: Why EventBridge Scheduler?**  
**A:** To run ingest on a **cadence** without a human. Frequency is a **cost knob** (Lambda invocations + S3 writes + projection sync LISTs).

**Q: Why Glue Data Catalog?**  
**A:** Athena requires a **catalog** to treat S3 prefixes as tables. We use **only** the metadata catalog—**not** Glue ETL jobs.

**Q: Why not Glue Crawler?**  
**A:** Crawlers **infer** schema and **discover** partitions; our tables use **partition projection** with explicit **`projection.run_id.values`**. A crawler would fight that model and add **S3 LIST** cost. We prefer **DDL in git** + **ALTER** after runs.

**Q: Why Athena?**  
**A:** SQL over Parquet in place—no load step into Redshift for routine questions. **Trade-off:** query discipline (filters) to control scan cost.

**Q: Why QuickSight?**  
**A:** Managed dashboards + SPICE for demo stability; integrates with Athena as a first-class data source.

**Q: Why manual DDL / deploy scripts?**  
**A:** **Infrastructure-as-code** for Glue: reproducible, reviewable, aligned with exact S3 layout. **`deploy_athena_v2.py`** patches `jmi_gold` → `jmi_gold_v2` and runs companion scripts for analytics views.

### 25.3 Medallion & keys

**Q: What is Bronze/Silver/Gold?**  
**A:** **Bronze** = raw vendor JSON lines + metadata; **Silver** = deduped job rows + strict schema; **Gold** = monthly aggregates for BI.

**Q: What is `run_id`?**  
**A:** Unique id for **one ingest batch** (`new_run_id()`—UTC timestamp + short id). It stamps Bronze and propagates for lineage.

**Q: What is `bronze_ingest_date`?**  
**A:** UTC **date** of the batch—used for ops partitions (`ingest_date=`) and lineage—not the same as business **`posted_month`**.

**Q: What is `posted_month`?**  
**A:** **Calendar month** of the job posting derived from `posted_at` when possible (`gold_time.py`). Gold facts partition on this for **business-aligned** monthly charts.

**Q: Why not partition Gold only by ingest date?**  
**A:** That would answer “jobs we **pulled** in March,” not “jobs **posted** in March”—wrong for market timing questions.

**Q: Why deterministic `job_id`?**  
**A:** So the same logical job hashes to the same key across runs → **dedupe** in Silver works.

### 25.4 Metadata & Athena behavior

**Q: What is partition projection?**  
**A:** Glue **TBLPROPERTIES** tell Athena how to **construct** partition paths without registering every partition in MSCK. Needs correct **`projection.run_id.values`** listing actual `run_id` folders.

**Q: Why did Athena sometimes return zero rows?**  
**A:** Common causes: **stale `run_id` enum**, wrong Glue **`LOCATION`**, or historical **`storage.location.template`**—see `docs/aws_live_fix_gold_projection.md`.

**Q: Why `latest_run_metadata` tables?**  
**A:** Single-file pointer to the current pipeline `run_id` per **source**—cheap for views/dashboards instead of scanning all partitions to infer “latest.”

**Q: Why `pipeline_run_summary`?**  
**A:** Per-month validation row counts—proof the Gold step produced coherent totals.

**Q: Why drop old `jmi_gold` / `jmi_analytics` Glue DBs?**  
**A:** Retire confusing v1 catalog entries; **archive** DDL under `archive_non_v2_ddl/`—**S3 data not deleted**.

### 25.5 Comparison & analytics semantics

**Q: What is strict-common?**  
**A:** Months where **both** EU and India have aligned “latest per month” rows—**fair** comparison. **Trade-off:** months only one side covers drop out—can look like “only April.”

**Q: What is HHI here?**  
**A:** Herfindahl-style concentration on **skill-tag demand shares** within a month/run/source. The SQL file warns: tags are **not** unique per job—interpret as **tag-mass concentration**, not perfect skill independence.

**Q: Why views instead of more Gold Parquet for comparison?**  
**A:** Comparison rules evolved during the project; views iterate faster than rewriting physical **`derived_*`** tables (some dropped in `comparison_v2_views.sql`).

**Q: Why not put all views in `jmi_gold_v2`?**  
**A:** **Separation of concerns** (facts vs presentation) and **QuickSight safety**—moving views changes dataset bindings unless you repoint everything.

### 25.6 Cost & ops

**Q: What was the cost anomaly about?**  
**A:** **S3 request** charges spiked (often shown under **APS3-Requests-Tier1** in ap-south-1 billing). Typical drivers: **LIST** during projection sync, **`aws s3 sync`**, frequent schedules. Mitigate: slower schedule, sync excludes, avoid redundant listing.

**Q: Why reduce schedule frequency?**  
**A:** Each run triggers Lambda + writes + **LIST-heavy** projection sync—directly ties to request volume.

### 25.7 Honest limitations & future work

**Q: Skill extraction limits?**  
**A:** Rule-based allowlist—**not** NLP embeddings. Good for MVP charts; would need ML + taxonomy governance for production HR analytics.

**Q: What would you improve next?**  
**A:** CI tests on fixtures, narrower projection sync (avoid scanning huge prefixes), richer geo normalization, automated Glue updates from pipeline output with audit trail.

---

## 26. Study map — files, commands, AWS pages, names

### 26.1 Files to know cold

| File | Why |
|------|-----|
| `src/jmi/paths.py` | Every canonical prefix |
| `src/jmi/pipelines/ingest_live.py` | Bronze |
| `src/jmi/pipelines/transform_silver.py` | Silver + merge |
| `src/jmi/pipelines/transform_gold.py` | Gold + incremental months |
| `src/jmi/pipelines/gold_time.py` | `posted_month` semantics |
| `src/jmi/connectors/skill_extract.py` | Skill rules |
| `src/jmi/aws/athena_projection.py` | S3 LIST → `ALTER TABLE` projection |
| `infra/aws/lambda/handlers/*.py` | Lambda chain |
| `scripts/deploy_athena_v2.py` | Glue DDL orchestration |
| `docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql` | Comparison logic |

### 26.2 Commands

| Command | Effect |
|---------|--------|
| `python -m src.jmi.pipelines.ingest_live` | Bronze batch |
| `python -m src.jmi.pipelines.transform_silver` | Silver from latest Bronze |
| `python -m src.jmi.pipelines.transform_gold` | Gold aggregates |
| `python scripts/pipeline_live_sync.py <source>` | Local pipeline + optional S3 sync + projection update |
| `python scripts/deploy_athena_v2.py` | (Re)create v2 Glue objects + views—**dangerous on shared accounts without review** |

### 26.3 AWS console pages

- **S3:** bucket → verify `bronze/`, `silver/jobs/`, `gold/` prefixes.  
- **Glue:** **Databases** → `jmi_gold_v2` → **Table properties** → `projection.*`.  
- **Athena:** **Query editor**—always set **Workgroup** + **output location** (`s3://…/athena-results/`).  
- **Lambda:** **Monitor** → **View logs in CloudWatch**.  
- **EventBridge Scheduler:** schedule **rate** and **enabled** state.  
- **Cost Explorer:** filter **S3** + **Request**-type metrics if available.

### 26.4 Athena names (v2)

- **Databases:** `jmi_gold_v2`, `jmi_silver_v2`, `jmi_analytics_v2`.  
- **Fact tables:** `skill_demand_monthly`, `role_demand_monthly`, `location_demand_monthly`, `company_hiring_monthly`, `pipeline_run_summary`.  
- **Pointers:** `latest_run_metadata_arbeitnow`, `latest_run_metadata_adzuna` (deployed under **`jmi_gold_v2`** via patch).  
- **Comparison views (examples):** `comparison_source_month_skill_tag_hhi`, `comparison_source_skill_mix_aligned_top20`, `comparison_benchmark_aligned_month` (see comparison SQL file for full list).

### 26.5 S3 prefixes (memory checklist)

`bronze/source=`, `silver/jobs/source=`, `gold/<table>/source=`, `gold/source=<slug>/latest_run_metadata/`, `quality/`, `health/`, `state/`, `athena-results/`, optional `derived/comparison/`, `silver_legacy/`, `gold_legacy/` (legacy/archive concepts).

---

## 27. Final rapid revision checklist

- [ ] Narrate **Bronze → Silver → Gold** with **one concrete path** including **`run_id`** and **`posted_month`**.  
- [ ] Explain **`run_id`** (batch lineage) vs **`bronze_ingest_date`** (ops date) vs **`posted_month`** (business month).  
- [ ] Explain **partition projection** and why **`sync_gold_run_id_projection_from_s3`** / **`ALTER TABLE`** happens after Gold.  
- [ ] Explain **strict intersection** and why a month can “disappear” from comparison.  
- [ ] State clearly: **dropping Glue metadata ≠ deleting S3 data**.  
- [ ] Acknowledge **QuickSight `dea final 9`** wiring is **console-verified**, not git-exported.

---

## Appendix A — Silver quality checks (exact logic)

`run_silver_checks` (`src/jmi/utils/quality.py`) fails if any of:

- Zero rows when Bronze had rows  
- Missing **title** (checks `title_norm` and limited legacy columns)  
- Missing **company** (`company_norm` / fallbacks)  
- Duplicate **`job_id`**  
- Duplicate **(source, source_job_id)**-style keys when present  

**Teacher angle:** “Silver is where we **enforce** data quality before aggregates.”

---

## Appendix B — Gold incremental behavior (live vs full)

- **`JMI_GOLD_INCREMENTAL_POSTED_MONTHS`:** comma months to rebuild (live sync sets a **rolling window** via `default_incremental_posted_months_live_window()`).  
- **`JMI_GOLD_FULL_MONTHS=1`:** rebuild all months found in Silver (`--full-posted-months` CLI flag).  
**Why:** Full history rebuild is expensive locally and in AWS—incremental is a **cost/latency** knob.

---

## Appendix C — Lambda payload shapes (for precise explanations)

**Ingest → Silver (async):** `{"bronze_file": "<path from ingest result>", "run_id": "<run_id>"}`  
**Silver → Gold (async):** `{"silver_file": "...", "merged_silver_file": "...", "run_id": "..."}`  
**Gold handler optional env overrides:** `source_name`, `incremental_posted_months`, `full_gold_months` via event keys (see `gold_handler.py`).

---

## Appendix D — `deploy_athena_v2.py` orchestration (what it actually runs)

1. `CREATE DATABASE jmi_gold_v2`  
2. `DROP` obsolete `latest_run_metadata` generic table name in v2  
3. For each `ddl_gold_*.sql`: patch `jmi_gold`→`jmi_gold_v2`, strip leading comments, execute  
4. `CREATE DATABASE jmi_analytics_v2`  
5. Subprocess: `deploy_jmi_analytics_v2_minimal.py`  
6. Subprocess: `deploy_athena_comparison_views_v2.py`  
7. Subprocess: `deploy_comparison_v2_views.py`  
8. Subprocess: `drop_presentation_layer_athena.py`  
9. Loop: `DROP VIEW` obsolete analytics names (list embedded in `deploy_athena_v2.py`)

**Viva:** “Deploy is scripted so Glue matches repo SQL—**repeatable** infrastructure.”

---

## Appendix E — Skill extraction internals (study pointers)

- Read **`SKILL_ALLOWLIST`** size and categories in `skill_extract.py` (tech stacks, data, security, soft skills buckets).  
- Understand **stoplist** removes generic hiring words.  
- **Adzuna:** `adzuna_enrich_weak_skills` may add signal when tags are thin—still rule-based.

---

## Appendix F — When NOT to use Streamlit

`dashboard/app.py` is great for **local QA** of Parquet. The **graded** demo path in docs is **QuickSight + Athena**—say: “Streamlit proves transforms locally; QuickSight is the presentation layer for stakeholders.”

---

## Appendix G — Exact Silver Parquet contract (canonical columns)

From `CANONICAL_SILVER_COLUMN_ORDER` in `silver_schema.py` (fixed order; extra legacy columns **stripped** by `project_silver_to_contract`):

1. `job_id`  
2. `source`  
3. `source_job_id`  
4. `title_norm`  
5. `company_norm`  
6. `location_raw`  
7. `remote_type`  
8. `skills` (stored as **JSON string** of array—see `_skills_to_json_str`)  
9. `posted_at`  
10. `ingested_at`  
11. `job_id_strategy`  
12. `bronze_run_id`  
13. `bronze_ingest_date`  
14. `bronze_data_file`  

**Viva:** “Silver has a **strict contract** so Gold and Athena never see surprise columns from old experiments.”

---

## Appendix H — Gold aggregate mechanics (what the code does)

For each `posted_month` in scope (`transform_gold.py`):

- **Skills (`_build_monthly_skill`):** `skills_json_to_list` → **explode** one row per (job, tag) → `groupby(skill)['job_id'].nunique()` → **`job_count` = distinct jobs listing that tag** in the month slice. If a job has 3 tags, it contributes to 3 skill rows—**not** comparable to “total jobs” without care.  
- **Roles (`_build_monthly_role`):** normalize `title_norm` → `groupby(role).size()` → job counts per role string.  
- **Locations:** `normalize_location_raw` then groupby.  
- **Companies:** from `company_norm` then groupby.  
- **`pipeline_run_summary`:** written per month slice with summary metrics for validation.

---

### Uncertainty log (do not present as facts)

- **QuickSight dashboard `dea final 9`:** Exact datasets/visuals **not** in repo.  
- **Live EventBridge enabled/disabled state** may differ from any doc snapshot—**check AWS**.  
- **Billing line naming** may vary slightly on invoice vs Cost Explorer labels.  
- **Exact row counts / which months exist** for each source in production S3—**verify** with Athena `SELECT DISTINCT posted_month` per source.
