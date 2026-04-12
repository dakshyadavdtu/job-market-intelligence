# Job Market Intelligence (JMI)

**Job Market Intelligence (JMI)** is an end-to-end data pipeline and analytics initiative. It ingests job-market data, stores it in a small **medallion-style data lake** (Bronze → Silver → Gold), and exposes **analytics-ready aggregates** for dashboards and SQL exploration. The **intended production style** is **AWS serverless** and **micro-batch** (frequent small runs rather than one giant nightly job). The repository today delivers a **working local slice** (one live API source, runnable pipelines, a Streamlit dashboard) plus **AWS-oriented assets** (Lambda handlers, IAM samples, EventBridge schedule definition, Athena DDL) that map how the same logic runs in the cloud.

This document explains the project **from problem to outcome**, **why the architecture looks the way it does**, and **how data moves through one batch**. It is written so someone new to AWS can follow the story; deeper operational detail lives under `docs/` and `infra/aws/`.

---

## 1. Project overview

JMI collects job postings from external sources, preserves them as **auditable raw snapshots**, refines them into **clean, deduplicated job rows**, and builds **curated aggregate tables** (skills, roles, locations, employers) suitable for charts and SQL. The same transformation code is designed to run **locally** (fast iteration) or **on AWS Lambda** (scheduled micro-batches writing to Amazon S3).

**In one sentence:** *Capture truth in Bronze, standardize in Silver, summarize in Gold, query in Athena, and present in a dashboard—without paying for idle servers.*

---

## 2. Problem statement

Job-market insight depends on **noisy, changing, duplicated** source data. If you clean too early, you **lose the ability to replay or audit** what the source actually returned. If you skip standardization, **every dashboard becomes a one-off hack**. If you query raw JSON at scale, **cost and performance suffer**.

JMI therefore separates concerns:

- **Ingestion** must be **faithful and cheap** (capture what the source said).
- **Standardization** must be **repeatable** (same rules every run).
- **Analytics** must be **small, fast, and partition-friendly** (Gold-first queries).

---

## 3. Final goal / expected outcomes

**Near-term (MVP):**

- Reliable **micro-batch** ingestion from at least **one live source** (today: **Arbeitnow**).
- **Partitioned** Bronze, Silver, and Gold datasets stored in **Amazon S3** (or locally under `data/` during development).
- **Glue Data Catalog** metadata and **Amazon Athena** SQL over **Gold** (and Silver when needed).
- A **dashboard layer**: today a **local Streamlit** app over Gold Parquet; later **Amazon QuickSight** (or equivalent) over Athena.

**Later phases (planned, not fully implemented in code):**

- **Historical CSV** backfills landed in S3 and ingested into the same Bronze contract.
- Additional **trend or macro signals** as separate `source=` partitions.
- Richer **BI** (shared dashboards, access control, scheduled refresh).

---

## 4. End-to-end architecture

**Conceptual flow (production target):**

```text
┌─────────────┐     ┌──────────────┐     ┌──────────────────────────────────────────────┐
│  Sources    │     │  Ingestion   │     │  Amazon S3 data lake (partitioned keys)       │
│  (API/CSV/  │────▶│  AWS Lambda  │────▶│  Bronze: raw JSONL.gz + manifest + health     │
│   signals)  │     │  (+schedule) │     │  Silver: Parquet jobs (cleaned, deduped)      │
└─────────────┘     └──────────────┘     │  Gold: Parquet aggregates (analytics-ready)   │
                              │          └──────────────────────────────────────────────┘
                              │                              │
                              ▼                              ▼
                     ┌──────────────┐              ┌─────────────────────┐
                     │ EventBridge  │              │ Glue Data Catalog   │
                     │ (4 hour rate)│              │ (tables/partitions) │
                     └──────────────┘              └──────────┬──────────┘
                                                              │
                                                              ▼
                                                   ┌─────────────────────┐
                                                   │ Amazon Athena (SQL) │
                                                   └──────────┬──────────┘
                                                              │
                                                              ▼
                                                   ┌─────────────────────┐
                                                   │ QuickSight /        │
                                                   │ dashboard app       │
                                                   └─────────────────────┘
```

**Local development** runs the **same pipeline modules** on your machine; data lands under `data/` instead of a shared S3 bucket. The dashboard app (`dashboard/app.py`) reads **latest Gold Parquet** from disk—useful before Athena/QuickSight are wired for every user.

---

## 5. Why this architecture was chosen

| Decision | Rationale |
|----------|-----------|
| **Serverless (Lambda + S3)** | No always-on clusters; cost tracks **invocations and storage**, aligned with a **micro-batch** cadence. |
| **Medallion (Bronze/Silver/Gold)** | **Separation of concerns**: auditability (Bronze), quality (Silver), performance for BI (Gold). |
| **Parquet for Silver/Gold** | Columnar format: **smaller scans**, better **Athena** economics than repeatedly parsing raw JSON. |
| **Glue + Athena** | **Serverless query** engine over S3; Glue holds the **table definitions** Athena needs. |
| **Gold-first analytics** | Dashboards and standard reports should hit **small aggregate tables**, not full job-level history every time. |
| **Strict lineage (`run_id`, paths)** | Every batch is **traceable**; reruns and debugging do not mix unrelated data silently. |

---

## 6. Source strategy

**Today (implemented):**

- **Arbeitnow** public API (`https://www.arbeitnow.com/api/job-board-api`) as the **first live source**. Connector and normalization live in `src/jmi/connectors/arbeitnow.py`.

**Planned extensions (design hooks, not full second pipeline in repo):**

- **Historical CSV**: land files under a controlled **S3 prefix**, validate schema, wrap rows into the **same Bronze envelope** (`raw_payload` + metadata), partition by `source=` and `ingest_date=`.
- **Trend / signal feeds**: ingest as **new `source=`** values; **do not merge** unrelated semantics into one Bronze folder.

**Rule:** new sources get **their own partition and connector**, but should **converge on a common Silver schema** so Gold and Athena stay stable.

---

## 7. Full workflow from source to dashboard

**Design time (people and repo):**

1. Agree scope, **cost cap**, and **layer contracts** (Bronze immutable, Silver rules, Gold aggregates).
2. Implement or extend **connectors** and **pipelines** under `src/jmi/`.
3. For AWS: package Lambdas, configure **S3 bucket**, **IAM**, **EventBridge** schedule (often **disabled** until validated).

**Runtime (one batch):**

1. **Trigger**: manual local run, manual Lambda invoke, or **EventBridge** on a schedule.
2. **Ingestion**: pull from the source API; assign **batch lineage**; write **Bronze** + **manifest** + **health pointer**. **No business cleaning** here.
3. **Silver**: read Bronze; **flatten** `raw_payload`; **normalize** fields (e.g. skills vocabulary); **deduplicate** by `job_id`; run **quality checks**; write **Parquet**.
4. **Gold**: read Silver; compute **aggregates** (skills, roles, locations, companies + a **pipeline summary**); partition by **`ingest_month`** (from lineage, not wall clock); write **Parquet** + **gold quality JSON**.
5. **Catalog & SQL**: register S3 paths as **Glue** external tables; run **Athena** queries (prefer **Gold**).
6. **Dashboard**: **Streamlit** locally from `data/gold/...` today; **QuickSight** against Athena later.

**AWS chain (as implemented in handlers):** ingest Lambda finishes Bronze, then **asynchronously invokes** Silver Lambda; Silver finishes, then invokes Gold Lambda. This keeps orchestration **simple** for the MVP (no Step Functions requirement in code today).

---

## 8. Bronze, Silver, and Gold in depth

### Bronze — raw, immutable snapshots

**Purpose:** Preserve **exactly what the source returned** (inside a thin envelope) so you can **replay**, **audit**, and **recover** if Silver rules change.

**What happens:** Each API job becomes a record with metadata (`source`, `schema_version`, `job_id`, timestamps, `run_id`, `bronze_ingest_date`, …) and a **`raw_payload`** field containing the **untouched** JSON object from the API.

**Outputs (local layout):**

- `data/bronze/source=arbeitnow/ingest_date=YYYY-MM-DD/run_id=<run_id>/raw.jsonl.gz`
- `data/bronze/source=adzuna_in/ingest_date=YYYY-MM-DD/run_id=<run_id>/raw.jsonl.gz` (Adzuna India; set `ADZUNA_APP_ID` / `ADZUNA_APP_KEY`, then `python -m src.jmi.pipelines.ingest_adzuna`)
- `data/bronze/.../manifest.json` (counts, paths, schema version)
- `data/health/latest_ingest.json` (pointer to the latest Arbeitnow batch)
- `data/health/latest_ingest_adzuna_in.json` (pointer to the latest Adzuna India batch)

**Why it exists:** If Silver drops a field you later need, **Bronze still has it**. If a vendor changes their schema, you can **detect drift** from historical Bronze.

### Silver — cleaned, standardized, deduplicated

**Purpose:** Turn semi-structured JSON into **typed, analytics-friendly rows** with **quality gates**.

**What happens:** Read Bronze JSONL.gz; map `raw_payload` into a **minimal Silver row** (normalized title/company, `location_raw`, `remote_type`, **`skills`** from allowlist/aliases/title/description with **Arbeitnow tag fallback** when needed, `posted_at`, plus `bronze_run_id` / `bronze_ingest_date` / `bronze_data_file` / `job_id_strategy`); **drop duplicate `job_id`**; **`project_silver_to_contract`** strips any legacy columns so Parquet matches the contract; run **`run_silver_checks`**; write **Parquet**. Display text, URL, long description, and `job_types` stay on **Bronze** (`raw_payload`). See `docs/data_dictionary.md`.

**Outputs:**

- `data/silver/jobs/ingest_date=YYYY-MM-DD/run_id=<run_id>/part-00001.parquet` (Arbeitnow — legacy flat layout)
- `data/silver/jobs/source=adzuna_in/ingest_date=YYYY-MM-DD/run_id=<run_id>/part-00001.parquet` (Adzuna India)

**Run (Adzuna Bronze → Silver):** `python -m src.jmi.pipelines.transform_silver --source adzuna_in`
- `data/quality/silver_quality_YYYY-MM-DD_<run_id>.json`

**Why it exists:** **All “cleaning” belongs here**, not in ingestion—so Bronze stays pure and Silver rules stay **testable**.

### Gold — analytics-ready aggregates

**Purpose:** Precompute **small fact tables** that dashboards and Athena can scan **cheaply**.

**What happens:** Read Silver; derive **`ingest_month`** from `bronze_ingest_date` (first seven characters, `YYYY-MM`); build:

- `skill_demand_monthly` — distinct jobs per skill  
- `role_demand_monthly` — jobs per normalized role title  
- `location_demand_monthly` — jobs per normalized location label  
- `company_hiring_monthly` — jobs per company (normalized)  
- `pipeline_run_summary` — one row with row counts and status for the batch  

**Outputs (examples):**

- `data/gold/skill_demand_monthly/ingest_month=YYYY-MM/run_id=<run_id>/part-00001.parquet`
- `data/gold/role_demand_monthly/...`
- `data/gold/location_demand_monthly/...`
- `data/gold/company_hiring_monthly/...`
- `data/gold/pipeline_run_summary/...`
- `data/quality/gold_quality_YYYY-MM_<run_id>.json`

**Run (Adzuna Silver → Gold):** `python -m src.jmi.pipelines.transform_gold --source adzuna_in` (reads `silver/jobs/source=adzuna_in/merged/latest.parquet`). **Does not** overwrite `gold/latest_run_metadata/` (that pointer stays Arbeitnow-oriented for existing Athena views).

**Why it exists:** **Gold-first analytics**—BI tools should not rescan all raw jobs for every chart refresh if a **small aggregate** answers the question.

---

## 9. AWS services used and what each does

| Service | Role in JMI |
|---------|-------------|
| **Amazon S3** | **System of record** for Bronze, Silver, Gold, quality JSON, health files, and (typically) Athena query results. |
| **AWS Lambda** | Runs **ingest**, **Silver transform**, and **Gold transform** without servers; same Python modules as local. |
| **Amazon EventBridge** | **Scheduler** (e.g. `rate(4 hours)` in `infra/aws/eventbridge/jmi-ingest-schedule.json`) to trigger ingest; should stay **disabled** until validated. |
| **AWS Glue Data Catalog** | **Metadata** (databases, tables, columns, partitions) so Athena knows **where** data lives in S3. |
| **Amazon Athena** | **SQL** over S3 via Glue; use **partition filters** and prefer **Gold** tables for routine reporting. |
| **Amazon QuickSight** (later) | **Shared dashboards** over Athena datasets / SPICE; enterprise consumption path. |
| **AWS IAM** | **Least-privilege** roles/policies for Lambda and EventBridge (see `infra/aws/iam/`). |
| **Amazon CloudWatch Logs** | **Operational visibility** for Lambda (errors, latency, tracing `run_id` in logs—recommended practice). |

**Not the goal:** long-running clusters, always-on databases for the MVP core path, or ad-hoc full scans of Bronze for every question.

---

## 10. Data flow and lifecycle of a single run

**Example: one successful local batch**

1. You run ingestion. The code generates a new **`run_id`** (UTC timestamp + short UUID) and **`bronze_ingest_date`** (UTC date).  
2. Every Bronze line includes those fields plus **`raw_payload`**.  
3. Silver picks the **latest Bronze file** (or a path passed explicitly on AWS), parses lineage from the path, flattens records, dedupes, runs **quality checks**, writes Silver Parquet **carrying `bronze_run_id`, `bronze_ingest_date`, `bronze_data_file`**.  
4. Gold reads that Silver file, computes aggregates, writes multiple Gold datasets under **`ingest_month=YYYY-MM`** and the same **`run_id`**.  
5. The Streamlit app (if run) picks the **latest Gold Parquet** per topic and charts **top skills/roles/locations/companies**.

On AWS, the **same sequence** runs in three Lambdas, with **S3 URIs** provided via **`JMI_DATA_ROOT`** (see `src/jmi/config.py`).

---

## 11. Canonical schema / important fields

**Bronze (each JSON line)** — see also `docs/data_dictionary.md`:

- **`source`**, **`schema_version`**, **`job_id`**, **`job_id_strategy`**, **`ingested_at`**
- **`raw_payload`**: full source document (unchanged)
- **Batch fields:** **`run_id`**, **`bronze_ingest_date`**, **`batch_created_at`**, etc.

**Silver (Parquet row)** — highlights:

- Identity: **`job_id`**, **`source`**, **`source_job_id`**
- Gold-oriented fields: **`title_norm`**, **`company_norm`**, **`location_raw`**, **`skills`**
- Other job facts: **`remote_type`**, **`posted_at`**, **`ingested_at`**
- Lineage / audit: **`bronze_run_id`**, **`bronze_ingest_date`**, **`bronze_data_file`**, **`job_id_strategy`**

**Gold (Parquet)** — typical columns:

- Dimension: **`skill`** / **`role`** / **`location`** / **`company_name`** (per table)
- Metric: **`job_count`**
- Lineage: **`source`**, **`bronze_ingest_date`**, **`bronze_run_id`**
- Partitions: **`ingest_month`** (and **`run_id`** in the path)

---

## 12. Deterministic `job_id`, lineage, and `run_id`

**Deterministic `job_id`:** Built in `src/jmi/connectors/arbeitnow.py` using a **stable hash** over canonical fields. Preference order: **source slug → URL → fallback** (title, company, location, created time). **`job_id_strategy`** records which path was used. This supports **idempotent deduplication** in Silver (same logical job → same key).

**`run_id` and `bronze_ingest_date`:** Created **once at ingest** and stamped on **every Bronze record**. Silver and Gold **propagate** these values. **Gold `ingest_month`** is derived from **`bronze_ingest_date`**, not from “today” at Gold time—so monthly partitions reflect **the batch’s ingest date**, not an accidental clock shift during transforms.

**Why it matters:** Dashboards and SQL can **filter to a specific batch**, compare batches, and **reproduce numbers** months later.

---

## 13. Partitioning and cost-efficiency

**Physical layout (Hive-style prefixes)** includes:

- Bronze: `source=.../ingest_date=.../run_id=.../`
- Silver: `jobs/ingest_date=.../run_id=.../`
- Gold: `<dataset>/ingest_month=.../run_id=.../`

**Practices:**

- **Always filter** Athena queries on **`ingest_month`**, **`ingest_date`**, and/or **`run_id`** when possible.  
- Prefer **Gold** tables for routine dashboards.  
- Keep Bronze **gzip JSONL**; Silver/Gold **Parquet** to reduce bytes scanned.  
- Plan **lifecycle rules** on old Bronze snapshots (see `docs/cost_guardrails.md`).

---

## 14. Athena + Glue workflow

1. **Create** (or use) an S3 bucket and the **same key layout** the pipelines write.  
2. **Register tables** in the **Glue Data Catalog** pointing at Bronze/Silver/Gold prefixes. DDL examples live in `infra/aws/athena/`.  
3. **Run Athena** in a workgroup whose results go to an **S3 scratch prefix** (e.g. `s3://.../athena-results/`).  
4. Build **views** that encapsulate “latest run” or “selected month” logic (patterns appear under `docs/dashboard_implementation/`).  

**Beginner note:** Glue is the **card catalog**; Athena is the **librarian** that fetches only the **pages (S3 objects)** your SQL references—**if** partitions are set up correctly.

---

## 15. Dashboard / QuickSight stage

**Today:** **`streamlit run dashboard/app.py`** reads the **newest** `part-*.parquet` under each Gold root on disk and shows **ranked tables and bar charts**, with **display-only** polishing for labels. It also surfaces **`data/health/latest_ingest.json`** when present.

**Target:** **Amazon QuickSight** (or similar) connected to **Athena**, with datasets built **only on Gold** (or Gold-backed views) so refreshes stay **cheap and consistent**. Checklists and metric notes exist under `docs/dashboard_implementation/` (including QuickSight build checklist and Athena view SQL).

---

## 16. Current project status

**Working today:**

- **Single live source:** Arbeitnow.  
- **Local pipelines:** ingest → Silver → Gold → Streamlit dashboard.  
- **Quality artifacts:** Silver and Gold JSON reports under `data/quality/`.  
- **AWS mapping:** Lambda handlers, packaging/deploy scripts, IAM policy samples, EventBridge JSON, Athena DDL under `infra/aws/`.

**In progress / later phase:**

- **Fully automated production** deployment and hardening (your account-specific wiring, alarms, lifecycle policies).  
- **QuickSight** dashboards as the primary consumer (docs exist; not required to run the local MVP).  
- **Additional sources** (CSV historical, trend feeds) as **separate connectors** and `source=` partitions.

This README is honest: the **core batch logic** is implemented and runnable locally; **cloud rollout** is **supported by repo assets** but is **not a one-click universal deploy** without your AWS context.

---

## 17. Repo structure

```text
job-market-intelligence-main/
├── README.md                 # This file
├── requirements.txt
├── dashboard/
│   └── app.py                # Streamlit UI over local Gold Parquet
├── src/jmi/
│   ├── config.py             # Paths, run_id, S3-capable DataPath
│   ├── connectors/
│   │   └── arbeitnow.py      # Fetch, bronze envelope, skill rules, job_id hash
│   ├── pipelines/
│   │   ├── ingest_live.py    # Bronze ingest
│   │   ├── transform_silver.py
│   │   └── transform_gold.py
│   └── utils/
│       ├── io.py             # JSONL.gz + Parquet (local / S3)
│       └── quality.py        # Silver quality checks
├── infra/aws/                # Lambda, IAM, EventBridge, Athena DDL, scripts
│   ├── lambda/handlers/      # ingest → silver → gold invoke chain
│   ├── eventbridge/
│   ├── iam/
│   ├── athena/
│   └── ...
└── docs/                     # Architecture, runbook, data dictionary, cost, dashboard specs
    ├── architecture.md
    ├── runbook.md
    ├── data_dictionary.md
    ├── cost_guardrails.md
    ├── project_full_workflow.csv
    └── dashboard_implementation/
```

---

## 18. Local workflow

**Prerequisites:** Python **3.11+**, virtual environment recommended.

**Exact order (matches implemented modules):**

1. Create and activate a venv.  
2. `pip install -r requirements.txt`  
3. `python -m src.jmi.pipelines.ingest_live`  
4. `python -m src.jmi.pipelines.transform_silver`  
5. `python -m src.jmi.pipelines.transform_gold`  
6. `streamlit run dashboard/app.py`  

**Environment:** Optional `JMI_DATA_ROOT` points the pipelines at a root path (local folder or `s3://bucket/prefix`). Default is `data/`.

**Expected outputs** (after one successful batch):

- Bronze: `data/bronze/source=arbeitnow/ingest_date=YYYY-MM-DD/run_id=<run_id>/raw.jsonl.gz` + `manifest.json`  
- Silver: `data/silver/jobs/ingest_date=.../run_id=.../part-00001.parquet`  
- Gold: `data/gold/{skill_demand_monthly,role_demand_monthly,location_demand_monthly,company_hiring_monthly,pipeline_run_summary}/ingest_month=YYYY-MM/run_id=.../part-00001.parquet`  
- Quality: `data/quality/silver_quality_*.json`, `data/quality/gold_quality_*.json`  
- Health: `data/health/latest_ingest.json`  

**Lineage rules (MVP):**

- Bronze **`run_id`** and **`bronze_ingest_date`** are created **once** during ingestion.  
- Silver carries **`bronze_run_id`**, **`bronze_ingest_date`**, **`bronze_data_file`**.  
- Gold **`ingest_month`** comes from Silver/Bronze lineage (**not** “now” at Gold time).

---

## 19. AWS workflow

**High-level operational order** (from `docs/runbook.md`; requires explicit approval in real accounts):

1. Account safety (MFA, profiles).  
2. **Budget alarms** and guardrails (`docs/cost_guardrails.md`).  
3. **S3 bucket** and prefixes for Bronze/Silver/Gold/quality/health.  
4. **IAM** least-privilege roles for Lambda and EventBridge.  
5. Deploy **Lambda** functions (packaging scripts under `infra/aws/lambda/`).  
6. Configure **EventBridge** schedule—example rule is **DISABLED** in `infra/aws/eventbridge/jmi-ingest-schedule.json` until validated.  
7. **Glue + Athena** DDL for external tables.  
8. **Manual validation** batch (compare S3 outputs, quality JSON, Athena counts).  
9. **Dashboard** verification (QuickSight or exported data).

**Handler chain:** `ingest_handler` → async **`silver_handler`** → async **`gold_handler`** (see `infra/aws/lambda/handlers/`).

**Important:** Nothing in the default **local** flow automatically executes AWS APIs unless **`JMI_DATA_ROOT`** is an **`s3://`** URI and credentials are present.

---

## 20. Validation and quality checks

- **Silver:** `run_silver_checks` verifies row presence, required text fields, and duplicate keys; failure **raises** and **blocks** Gold in strict local runs. Metrics persist to **`silver_quality_*.json`**.  
- **Gold:** After aggregates, **`gold_quality_*.json`** records output paths and row counts for audit.  
- **Operational:** Review **CloudWatch Logs** for Lambda; compare **manifest `record_count`** to Silver/Gold summaries.  
- **Athena:** Use **partition filters**; cross-check distinct job counts against Gold summaries when building trust.

---

## 21. Cost guardrails

Documented in **`docs/cost_guardrails.md`**. Principles:

- Hard **project spend cap** (e.g. **≤ $3** total for the referenced MVP posture).  
- **Serverless-only** compute for the core path.  
- **Compressed** Bronze; **Parquet** Silver/Gold.  
- **Partition pruning mandatory** in Athena.  
- **Gold-first** dashboard queries.  
- Avoid unnecessary **Glue crawlers**; prefer **explicit DDL** in small projects.  
- Use **lifecycle** retention on old Bronze where policy allows.

---

## 22. How more sources will be added later

1. **New connector** (or CSV loader) producing the **same Bronze envelope** pattern with a distinct **`source`** and **`source=`** partition.  
2. **Silver mapping** extended to normalize vendor-specific fields into the **shared schema** (branch on `source` if needed).  
3. **Gold** continues to group by dimensions; use the existing **`source`** column in filters or separate dashboards to avoid **blind mixing**.  
4. **Hashing / `job_id`**: ensure **cross-source collisions** are impossible (e.g. **namespace prefix** in hash inputs per vendor if ever required).

---

## 23. Future enhancements

- **Second and third sources** (CSV backfill, trend signals) with shared Silver/Gold contracts.  
- **Step Functions** or queue-based orchestration if async Lambda chaining needs stronger observability.  
- **Great Expectations**-style tests or **CI** running Silver/Gold on fixture Bronze.  
- **Row-level security** in QuickSight for multi-tenant scenarios.  
- **Lifecycle automation** and **data retention** policies per compliance needs.

---

## 24. How to understand this project as a beginner

1. Read **sections 7–10** above—the **story** of one batch.  
2. Run the **local workflow** once and open the **output paths** on disk.  
3. Open **`src/jmi/pipelines/ingest_live.py`**, then **`transform_silver.py`**, then **`transform_gold.py`** in order; each file is one stage.  
4. Skim **`docs/project_full_workflow.csv`** for a **row-by-row lifecycle** map.  
5. When ready for AWS, read **`docs/runbook.md`** and **`infra/aws/README.md`**, then **Glue/Athena DDL** under **`infra/aws/athena/`**.

**Mental model:** **Bronze = evidence**, **Silver = trustworthy rows**, **Gold = fast answers**, **Athena = questions**, **Dashboard = pictures**.

---

## License / contributions

Add your organization’s **license** and **contribution** guidelines here if applicable.
