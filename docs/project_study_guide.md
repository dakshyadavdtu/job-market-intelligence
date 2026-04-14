# Job Market Intelligence (JMI) — Deep Study Guide

**Purpose:** Internal study manual for end-to-end understanding and viva preparation. It is grounded in **this repository** and documented AWS patterns—not generic cloud theory.

**How to use:** Read sections 1–11 for the core story; use 12–23 for Athena, BI, costs, incidents, and exam-style Q&A.

---

## 1. Project in one simple paragraph

**Job Market Intelligence (JMI)** is a data pipeline that pulls job postings from external APIs, stores them in a **medallion-style lake** on **Amazon S3** (Bronze → Silver → Gold), and serves **aggregated, SQL-queryable** metrics through **AWS Glue + Amazon Athena** to **Amazon QuickSight** dashboards (and a **local Streamlit** app for development). The **problem** it solves is that raw job data is noisy, duplicated, and fast-changing—so you need **auditable raw storage**, **cleaned job-level rows**, and **small monthly aggregates** for charts without scanning millions of JSON lines every time. The **final output** is **analytics-ready Parquet** plus **Glue catalog metadata** and **dashboards** that answer “what skills, roles, locations, and employers matter—by month and pipeline run?”

---

## 2. Problem statement

**Why job market data is hard to use directly**

- **Fragmented:** Different APIs (Arbeitnow EU, Adzuna India, future CSVs) use different JSON shapes, field names, and time fields.
- **Noisy:** Titles and tags are inconsistent; “React” vs “react.js”; location strings are free text.
- **Duplicate-heavy:** The same job can reappear across pages or runs; without a **deterministic job id** and dedupe rules, counts double-count.
- **Changing fast:** Sources add fields or pagination; if you only keep “clean tables” and throw away raw payloads, you **cannot replay** Silver/Gold when rules improve.

**Why this project helps students / placement / mentors**

- It turns feeds into **stable KPIs**: skill demand, role mix, geography, employer concentration—**per month** and **per batch (`run_id`)**, so you can say “this number came from this run.”
- It demonstrates a **realistic analytics engineering** path: lake → catalog → SQL → BI, with **cost and lineage** in mind.

---

## 3. Full end-to-end project flow

| Stage | What goes in | What happens | What comes out | Why it exists |
|-------|----------------|--------------|----------------|---------------|
| **Sources** | HTTP APIs (Arbeitnow, Adzuna), (planned) CSV | Connectors fetch pages; assign **run_id**, **bronze_ingest_date** | In-memory job dicts | Defines the **system boundary** from vendor to your code |
| **Ingestion** | API JSON | Wrap each job in a **Bronze envelope**; **no business cleaning**; gzip JSONL | `bronze/source=<slug>/ingest_date=…/run_id=…/raw.jsonl.gz` + manifest | **Immutable audit trail**—replay if Silver rules change |
| **Bronze** | (same as ingestion output) | Stored as **source of truth** | Compressed JSONL + optional health pointers under `health/` | Prove **what the API actually returned** |
| **Silver** | Bronze JSONL | Flatten `raw_payload`, normalize fields, **rule-based skills**, dedupe by `job_id`, quality checks | `silver/jobs/source=<slug>/…/part-00001.parquet` + merged `latest.parquet` + `quality/silver_quality_*.json` | **One row per job** with a **strict Parquet contract** |
| **Gold** | Silver (merged or union) | Aggregate by **posted_month** (calendar month from `posted_at`), by skill/role/location/company; write **pipeline_run_summary**; write **latest_run_metadata** pointer per source | `gold/<table>/source=<slug>/posted_month=…/run_id=…/part-00001.parquet` + `gold/source=<slug>/latest_run_metadata/` + `quality/gold_quality_*.json` | **Small tables** for BI; predictable scan size in Athena |
| **Glue Data Catalog** | S3 paths | Tables/views registered (DDL or API) | Databases: **`jmi_gold_v2`**, **`jmi_silver_v2`**, **`jmi_analytics_v2`** (and historically v1—see §11) | Athena needs **schema + partition projection** metadata |
| **Athena** | Glue + S3 | SQL over Parquet; views encode “latest run”, comparisons, KPI slices | Query results in **`s3://…/athena-results/`** | **Ad-hoc and dashboard** queries without a warehouse cluster |
| **QuickSight / dashboard** | Athena datasets | SPICE or Direct Query; visuals bound to datasets | Shared dashboards (e.g. multi-region comparison) | **Presentation** layer for non-SQL users |

**Local path:** The same Python modules write under `data/` instead of `s3://` when `JMI_DATA_ROOT` is local.

---

## 4. Actual source strategy in this project

| Topic | Fact in this repo |
|-------|-------------------|
| **Intended breadth** | Multiple **`source=`** partitions (EU Arbeitnow, India Adzuna, future CSV/trend feeds). |
| **Implemented live sources** | **Arbeitnow** (`arbeitnow`) — primary EU board API. **Adzuna India** (`adzuna_in`) — full Bronze→Silver→Gold path; see `docs/adzuna_india_runbook.md`. |
| **Planned / partial** | Historical CSV backfills, extra “signal” feeds—as **new sources**, not mixed into one ambiguous folder. |
| **Why Arbeitnow matters** | First **public API** used end-to-end; drives default pipeline commands and much of the EU dashboard story. |
| **Adzuna / comparison** | Second source enables **EU vs India** comparisons in **`jmi_analytics_v2`** views (`ATHENA_VIEWS_COMPARISON_V2.sql` lineage). Skill/tag **HHI** and aligned top-20 mix views are defined there. |
| **Historical / trend** | Documented as **future** in `README.md`; hooks are **`source=`** partitions and shared Silver schema—not implemented as separate production feeds in code reviewed here. |

---

## 5. Bronze / Silver / Gold explained deeply

### Bronze

- **In general:** Raw, append-only, lossless (within envelope) capture.
- **In JMI:** Each line has metadata + **`raw_payload`** = exact vendor JSON. **Salary, long description, URLs** stay here if not in Silver contract.
- **Paths:** `bronze/source=<slug>/ingest_date=YYYY-MM-DD/run_id=<id>/raw.jsonl.gz`.
- **Partitions:** `ingest_date` (UTC date of batch), `run_id` (unique batch id).
- **Why not skip:** Without Bronze, you cannot **audit** dedupe or fix Silver without re-fetching history.

### Silver

- **In general:** Clean, typed, **deduplicated** job-level table.
- **In JMI:** Columns are strictly controlled (`silver_schema` + `project_silver_to_contract`). **`skills`** come from **rule-based** extraction (`skill_extract.py`), not from arbitrary tag dumps alone.
- **Paths (active):** `silver/jobs/source=<slug>/ingest_date=…/run_id=…/part-00001.parquet`; **`merged/latest.parquet`** for downstream Gold. Legacy flat **`silver/jobs/ingest_date=…`** was treated as legacy; **active writers** use **source-prefixed** layout (see `src/jmi/paths.py`, `docs/STORAGE_LAYOUT_MULTISOURCE.md`).
- **Lineage preserved:** `bronze_run_id`, `bronze_ingest_date`, `bronze_data_file`, `job_id_strategy`.
- **Why not skip:** Gold aggregates would inherit **dirty text** and unstable keys; costs would explode if every chart scanned Bronze JSON.

### Gold

- **In general:** **Aggregates** for BI—small row counts, partition-friendly.
- **In JMI:** Fact tables: skill/role/location/company **monthly** demand; **`pipeline_run_summary`** per posted_month slice; **`latest_run_metadata`** single-row pointer per **source** under `gold/source=<slug>/latest_run_metadata/`.
- **Partition key for facts:** **`posted_month`** (YYYY-MM)—calendar month derived from Silver `posted_at` / time-axis logic—not “today’s month” at run time. Glue DDL uses **`posted_month`** in partition projection (`infra/aws/athena/ddl_gold_*.sql`).
- **Why not skip:** Dashboards would re-aggregate from Silver every refresh—slow, expensive, inconsistent.

---

## 6. S3 structure in this project

**Typical bucket layout (conceptual)**

| Prefix | Role |
|--------|------|
| `bronze/` | Raw JSONL.gz by `source=` / `ingest_date=` / `run_id=` |
| `silver/` | Job-level Parquet; **`jobs/source=<slug>/…`** active layout |
| `gold/` | Fact tables + **`source=<slug>/posted_month=`** / `run_id=`; metadata under **`gold/source=<slug>/latest_run_metadata/`** |
| `silver_legacy/` | Optional archive for **pre–source-prefix** Arbeitnow batches (flat under `jobs/`)—kept **out of** ambiguous `silver/jobs/` mixing |
| `gold_legacy/` | Documented for old **`ingest_month=`**-style paths—not written by current modular pipeline (`paths.py` comments) |
| `derived/` | **Comparison / benchmark** outputs that are **not** source-native Gold (keep separate from `gold/source=*` facts) |
| `quality/` | JSON quality reports (Silver/Gold) when written by local or synced runs |
| `health/` | Small JSON **pointers** to latest ingest (e.g. `latest_ingest.json`) |
| `athena-results/` | Athena query scratch output (by convention same bucket) |
| `state/` | Optional pipeline state if used |

**Active vs legacy**

- **Active:** `source=` **everywhere** for new Bronze/Silver/Gold facts; **`posted_month`** on Gold partitions in v2 DDL.
- **Legacy:** Flat `silver/jobs/ingest_date=` without `source=`; old **`jmi_silver.jobs`**-style Hive layouts; v1 Glue DBs **`jmi_gold` / `jmi_silver` / `jmi_analytics`** were **removed from the catalog** in a deliberate cleanup—definitions **archived** under `infra/aws/athena/archive_non_v2_ddl/` (S3 data untouched).

**Why `source=` and `posted_month=` matter**

- **`source=`:** Multi-source lake without ambiguous mixed prefixes; Athena projection uses **`projection.source.values`** (e.g. `arbeitnow,adzuna_in`).
- **`posted_month=`:** Aligns charts to **“when the job was posted”** (business month), not only **when you ingested** (`bronze_ingest_date` still in row lineage).

---

## 7. Important schemas and data contracts

**Identity**

- **`job_id`:** Deterministic hash from stable fields (see `connectors/arbeitnow.py` / Adzuna)—supports **idempotent dedupe** in Silver.
- **`job_id_strategy`:** Audit trail for which hash inputs were used.

**Source and runs**

- **`source`:** Slug (`arbeitnow`, `adzuna_in`).
- **`run_id`:** One per ingest batch (UTC timestamp + short id)—propagates to Bronze/Silver/Gold paths.
- **`bronze_ingest_date`:** UTC date string for the batch.

**Time**

- **`posted_at`:** Job posting time from vendor (Silver).
- **`posted_month`:** Gold partition key—**YYYY-MM** from time-axis assignment (`gold_time.py`), not wall-clock at Gold time.

**Skills**

- **`skills`:** Array of strings from **allowlist / aliases / stoplist** + context (`skill_extract.py`)—not free-form NLP.

**Why deterministic `job_id` matters:** Same logical job → same key → **dedupe**; without it, monthly aggregates **double-count**.

**Why lineage matters:** Every Gold row can be tied to **which Bronze file** and **which run** produced it—needed for debugging and viva “reproducibility” questions.

---

## 8. Detailed pipeline logic (concrete)

### `ingest_live` (Arbeitnow)

- Fetches API pages, builds Bronze lines, writes **`raw.jsonl.gz`**, **`manifest.json`**, updates **`health/latest_ingest.json`** (local).
- Produces **`run_id`**, **`bronze_ingest_date`**, path to Bronze file.

### `ingest_adzuna` (India)

- Separate module; same Bronze envelope pattern under **`source=adzuna_in`**.

### `transform_silver`

- Resolves **latest Bronze** (or explicit path on AWS).
- Maps to Silver rows; **`extract_silver_skills`**; Adzuna-specific helpers for title/location/skills.
- Dedupes on **`job_id`**; **`run_silver_checks`**; writes batch Parquet + **merged/latest.parquet**; **`silver_quality_*.json`**.
- For Arbeitnow, can **union** historical batches (including legacy flat paths) when building a broad Silver history—see `load_silver_jobs_history_union` usage in `transform_gold.py`.

### `transform_gold`

- Resolves Silver: **merged** vs **union** vs explicit file—prefers broader **`posted_month`** span when needed (commentary in `_resolve_silver_dataframe`).
- Assigns **`posted_month`**; loops months; writes **five fact families** + **pipeline_run_summary** via `gold_fact_partition`.
- Writes **`latest_run_metadata`** Parquet for **this source only** (does not overwrite the other source’s pointer).
- **`JMI_GOLD_INCREMENTAL_POSTED_MONTHS`** (live sync): limits which months to rebuild—cost/latency control.

### Quality / health

- **`data/quality/`** JSON: row counts, paths, validation outcomes.
- **`data/health/`**: small “latest ingest” pointers for ops/dashboard shell.

---

## 9. Skill extraction logic

**How it works today**

- Implemented in **`src/jmi/connectors/skill_extract.py`**: **allowlist** (`SKILL_ALLOWLIST`), **aliases**, **stoplist**, phrase/token matching over **tags + title + description** context.
- **No ML model** in the default path—explicitly rule-based for MVP predictability.

**Why generic words were a problem**

- Without stoplists/allowlists, common words (“data”, “team”, “management”) inflate **fake skills** and distort demand charts.

**Improvements in spirit of the project**

- Canonicalization toward a **controlled vocabulary**; Adzuna path can **enrich** weak skill signals where documented (`adzuna_enrich_weak_skills` pattern in `transform_silver.py`).

**Limitations**

- Not **NER** or deep NLP—misses novel phrases; may over-map to nearest allowlisted token.
- **Acceptable for MVP:** reproducible, cheap, explainable; **not** production HR-grade taxonomy.

---

## 10. Gold outputs and what each one means

| Output | Meaning |
|--------|---------|
| **`skill_demand_monthly`** | Distinct-job counts per **skill** × `posted_month` × `source` × `run_id` (tag grain—not deduped across tags per job in some analytics; comparison docs warn where relevant). |
| **`role_demand_monthly`** | Jobs per **normalized role/title** grain. |
| **`location_demand_monthly`** | Jobs per **normalized location** label. |
| **`company_hiring_monthly`** | Jobs per **normalized company**. |
| **`pipeline_run_summary`** | Per–posted-month slice: row counts and status for **validation** (“did this run produce sensible totals?”). |
| **`latest_run_metadata` / v2 table names** | **Single-row** Parquet (or Glue table pointing at it) with **current** `run_id` for that **source**—drives “latest run” views. **Latest run** = pointer chosen by pipeline, not “max run_id in SQL” alone (though comparison SQL often uses **MAX(run_id)** per month for alignment). |

**Why `pipeline_run_summary` exists:** Proof layer for demos and debugging—reconcile job counts vs Silver without hand-counting Parquet.

**Why `latest_run_metadata` exists:** QuickSight/Athena “latest” dashboards need a **stable, cheap** place to read **`run_id`** without scanning all partitions.

**Month-wise vs `run_id`:** Each Gold partition is **`posted_month` + `run_id`**. Multi-month charts aggregate across months **for the same or aligned runs** depending on view logic (EU-only vs strict-common intersection—see §12).

---

## 11. Athena layer in this project

**Why Athena:** Serverless SQL over S3 Parquet with **partition pruning** (when predicates match projection).

**Databases (intended current direction)**

- **`jmi_gold_v2`:** **External tables** for Gold facts + latest-run metadata tables (partition **projection** on `source`, `posted_month`, `run_id`).
- **`jmi_silver_v2`:** Silver-backed tables/views (e.g. merged jobs) where deployed.
- **`jmi_analytics_v2`:** **Views** (and helpers) for KPI slices, EU/India dashboards, **comparison**—built from SQL files in `infra/aws/athena/` and `docs/dashboard_implementation/`.

**Old non-v2 databases (`jmi_gold`, `jmi_silver`, `jmi_analytics`):** **Dropped from the Glue catalog** in a controlled cleanup; **archive** of DDL + Glue JSON snapshots: `infra/aws/athena/archive_non_v2_ddl/`. **S3 data remains.**

**Why views:** Encode **latest run**, **top-N**, **Pareto**, **comparison windows**—without materializing extra Parquet for every change.

**Why delete carefully:** QuickSight datasets reference **`database.view`**; dropping views **breaks SPICE refresh** until datasets are updated.

---

## 12. Comparison layer (Europe vs India) — deep

**Concepts**

- **Strict-common (months):** Months where **both** regions have data under **aligned “latest per month”** logic—see `intersection` CTEs in `ATHENA_VIEWS_COMPARISON_V2.sql`.
- **Latest aligned benchmark:** Uses **role_demand_monthly** to pick **MAX(run_id) per posted_month** per source within a **rolling month window** (`month_bounds` in comparison SQL—typically **previous calendar month through current month** as implemented in file). **Not** “all history forever” unless you change that window.

**Why March/April-type issues appeared**

- If one source’s **latest run** did not include **`posted_month`** for a month the other had, that month **drops out of intersection**—charts look “only April” or miss a bridge month. This is a **data coverage + pointer** issue, not random SQL noise.

**HHI (Herfindahl-Hirschman Index) on skills**

- **Skill-tag HHI (in `comparison_benchmark_aligned_month`):** Concentration of **tag-demand mass** across skills (same formula as the former standalone HHI helper; **not** deduped per job for tag rows). Higher HHI ⇒ fewer skills dominate the tag distribution.

**Why view-heavy**

- Comparison logic **changes** with teacher feedback; views are **fast to iterate** vs rewriting Parquet pipelines for every tweak.

**Which views feed dashboards**

- Repo documents **`comparison_source_skill_mix_aligned_top20`**, **`comparison_benchmark_aligned_month`** (skill-tag HHI inlined in the benchmark view) as the **minimal comparison set** for a “dea final 6”-style build—**exact** QuickSight wiring is **account-specific** (see §13 uncertainty).

---

## 13. QuickSight / dashboard layer

**What exists in-repo vs in AWS**

- The repo has **checklists** (`docs/dashboard_implementation/QUICKSIGHT_BUILD_CHECKLIST.md`, `QUICKSIGHT_MULTILAYER_BUILD.md`) and **Athena view SQL**.
- **Dashboard asset names** (e.g. **`dea final 9`**) and **exact dataset→visual wiring** live in **QuickSight**, not in git—**verify in the QuickSight UI** for your account.

**Imported vs actually used**

- The inventory doc warns about **duplicate datasets** and orphan analyses—**imported** datasets may exist without being on the published dashboard.

**SPICE vs Direct Query**

- Checklist: **SPICE** for stable demos with scheduled refresh; **Direct Query** for dev—refresh cost ties to how often data and SPICE sync run.

**Sections (conceptual)**

- **Europe (Arbeitnow):** KPIs, skills, roles, locations, companies—views like `sheet1_kpis` pattern in v1 docs; v2 uses `jmi_gold_v2` + `jmi_analytics_v2` equivalents per deploy scripts.
- **India (Adzuna):** Map/heat/box/scatter helpers—`analytics_v2_adzuna_*.sql` family.
- **Comparison:** EU vs India totals and skill mix—`ATHENA_VIEWS_COMPARISON_V2.sql`.

---

## 14. Visuals used in the project (intent)

| Visual type | Typical use in JMI |
|-------------|-------------------|
| **KPI cards** | Headline totals, shares (top-3 location share, etc.) |
| **Heat map** | State × skill or region × month (India helpers) |
| **Radar** | Profile comparison where `analytics_v2_adzuna_radar_helper.sql` supports it |
| **Sankey** | Flow visuals where EU Sankey helpers exist (`analytics_v2_eu_sankey_helper.sql`) |
| **Treemap** | Employer or location concentration |
| **Donut / pie** | Skill mix composition |
| **Scatter / bubble** | City metrics (India scatter helpers) |
| **Histogram** | Employer-size distribution (multilayer doc—EU hiring grain) |
| **Box plot** | Skill job-count distribution across jobs |
| **Line / area** | Month trends, comparison totals |
| **Stacked / clustered bars** | Side-by-side regions or sources |
| **HHI** | Concentration metrics on skill-tag distribution |

**Avoided / weak**

- Docs sometimes **demote** duplicate treemap vs bar when two visuals tell the same “inequality” story—preference for clarity over chart count.

---

## 15. AWS services used and why

| Service | Role in JMI |
|---------|-------------|
| **S3** | System of record for Bronze/Silver/Gold/quality/health/athena-results |
| **Glue Data Catalog** | Tables/views for Athena; **partition projection** metadata |
| **Athena** | SQL analytics; **views** for BI semantics |
| **QuickSight** | Dashboards; SPICE/DQ datasets on Athena |
| **Lambda** | **ingest → async invoke Silver → async invoke Gold** (`infra/aws/lambda/handlers/`) |
| **EventBridge Scheduler** | Periodic trigger (`infra/aws/eventbridge/jmi-ingest-schedule.json`)—**rate** documented as **24 hours** in file (schedule **name** may still say “10min” historically—misleading; verify live) |
| **IAM** | Least-privilege roles for Lambda/EventBridge |
| **CloudWatch Logs** | Lambda stdout/stderr for ops |
| **Billing / Budgets / Cost Anomaly** | **Recommended** guardrails in `docs/cost_guardrails.md`—not application code |
| **CloudShell** | Convenience for CLI in console—optional |

**Not core**

- **Glue Crawler / Glue ETL jobs:** Not the main path—metadata is **DDL/deploy scripts**; transforms are **Python in Lambda/local** (defensible: control, cost, projection alignment).

---

## 16. Cost architecture and cost incidents

**Why low cost matters:** Student / capstone budget—**serverless pay-per-use**.

**Sensitive choices**

- Parquet + partition filters; **Gold-first** queries; **incremental Gold months** in live sync; **no** idle clusters.

**S3 request anomaly (practical framing)**

- **APS3-Requests-Tier1** (billing line name) reflects **per-request** charges for Standard S3 in **ap-south-1**—typically **GET/PUT/LIST**-like operations depending on class.
- **Causes that fit this project:** frequent **`aws s3 sync`**, listing large prefixes, aggressive schedules, Athena/Glue patterns that **list** more than needed, or tools that **HEAD/LIST** repeatedly. **Exact** root cause needs **Cost Explorer + CloudTrail/S3 access patterns** for your account—do not invent a single cause.

**Schedule reduction**

- Moving from overly frequent triggers to **daily** (or disabling schedule during validation) reduces **Lambda + downstream S3 writes**.

**What increases S3 request cost**

- Full-prefix listings, sync without excludes, crawlers (if used), excessive small GETs in automation.

**Mitigation**

- Sync **excludes** for legacy keys (see `pipeline_live_sync.py` patterns), **lifecycle** on old Bronze (`cost_guardrails.md`), **rationalize schedule**.

---

## 17. Important design decisions and trade-offs

| Decision | Trade-off |
|----------|-----------|
| **Lambda + S3 vs Glue ETL** | Lambda fits **short** batch transforms; no DPU cluster to manage; **limit:** timeouts/package size for huge data. |
| **Manual DDL vs crawler** | **Controlled projection enums** (`projection.run_id.values`) vs auto schema—picked **manual** for stability. |
| **Local-first MVP** | Fast iteration; **AWS** for demo scale. |
| **Streamlit local** | Quick visual QA without QS; **not** the graded enterprise BI path. |
| **QuickSight** | Managed BI; **cost + learning curve**; SPICE refresh discipline required. |
| **View-based comparison** | Fast iteration; **heavier Athena** if views are wide—acceptable at student scale with filters. |
| **Strict-common logic** | Fair comparison but **drops months** without dual coverage. |
| **Source-prefixed layout** | Clear multi-source lake; **migration effort** from legacy paths. |
| **Legacy cleanup** | Archived DDL + catalog drop—**S3 untouched** for safety. |

---

## 18. Project evolution / mistakes / fixes (honest)

| Issue | What went wrong | Fix / lesson |
|-------|-----------------|--------------|
| **March missing in some views** | **Intersection** of months or **rolling window** excluded months one source lacked for `posted_month`. | Adjust comparison SQL/window; ensure both sources have coverage for target months; **validate** `role_demand_monthly` per month. |
| **Latest-run pointer vs projection** | Athena returned **no rows** when Glue had wrong **`storage.location.template`** or **`projection.run_id.type`** incompatible with JOIN-based latest-run views. | Doc: `docs/aws_live_fix_gold_projection.md`—use **`enum`** for `run_id`, append new run ids, remove bad template. |
| **Inconsistent Silver layout** | Flat **`silver/jobs/ingest_date=`** vs modular **`source=`** caused confusion and accidental flat writes from legacy table patterns. | **Source-prefixed** active layout; purge/DDL hygiene; **`silver_legacy`** for archived flat batches. |
| **Geo / map fields** | QuickSight maps need recognizable **geographic fields**; bad column names break choropleth. | Normalize **state/city** fields in helpers (`analytics_v2_adzuna_geo_helpers.sql` etc.); verify in QS dataset preview. |
| **Strict-common became “one month”** | Latest month in intersection can be **only April** if March not aligned—**looks** like “April-only comparison.” | Explain **data availability**; widen window or fix run coverage—not just “chart bug.” |
| **Duplicate comparison views** | Multiple experimental view names for same semantic. | Deploy scripts drop **redundant** aliases (`deploy_jmi_analytics_v2_minimal.py` notes). |
| **Cost anomaly** | High **request** volume on S3. | Reduce sync frequency, exclude prefixes, review schedule; inspect billing dimension. |
| **Skill extraction quality** | Generic tokens polluted skills. | Allowlist/stoplist + richer rules; still not perfect NLP. |

---

## 19. Current project status (as documented in repo)

**Working**

- End-to-end **local** pipelines; **AWS** Lambda chain + EventBridge JSON; **Athena v2** DDL + analytics deploy scripts; **comparison** view SQL.

**Rough / operational**

- **Account-specific** IAM, QuickSight ownership, exact schedule **enabled/disabled** in AWS may differ from any single doc snapshot.

**Planned**

- More sources, stronger tests, optional Step Functions—**roadmap** tone in `README.md`.

**Legacy**

- v1 Glue DBs **dropped** from catalog (archived); legacy S3 keys may still exist until lifecycle deletes.

**Should remain**

- **`jmi_gold_v2` / `jmi_silver_v2` / `jmi_analytics_v2`** separation story for clarity.

**Archive**

- `infra/aws/athena/archive_non_v2_ddl/` for **reconstructing** old Glue if ever needed.

---

## 20. How to explain this project in viva

**30 seconds**  
“We ingest public job APIs into S3 as Bronze, clean to Silver Parquet with deterministic job ids, aggregate to monthly Gold facts, register Glue tables with partition projection, and visualize through Athena into QuickSight—so we get traceable KPIs per pipeline run without a traditional database cluster.”

**1 minute**  
Add: **Arbeitnow + Adzuna** as two sources, **`source=` partitioning**, **`run_id` lineage**, **views** for latest-run and EU/India comparison, **cost-aware** serverless choices.

**3 minutes**  
Add: **medallion rationale**, **skill extraction limits**, **one real incident** (projection or intersection month), **what you’d improve next** (tests, schedule hardening, taxonomy).

---

## 21. Likely teacher questions and strong answers (project-specific)

**Why this project?**  
Job postings are messy and fast-changing; we need **auditability** and **repeatable KPIs** for placement insight.

**Why AWS?**  
**S3 + serverless query + managed BI** match small-budget, no-ops cluster; scales to demo data volume.

**Why S3?**  
Durable, cheap object storage for **immutable Bronze** and **columnar Parquet** for analytics.

**Why Lambda?**  
Fits **micro-batch** transforms; same code as local; chains with async invoke.

**Why Bronze/Silver/Gold?**  
**Separation of concerns**: evidence → clean rows → aggregates.

**Why Athena?**  
SQL over Parquet **without loading a warehouse**; works with Glue catalog.

**Why QuickSight?**  
Managed dashboards for non-developers; SPICE for demo stability.

**Why not Glue Crawler?**  
We use **partition projection + explicit DDL**; crawlers add **cost**, **schema drift**, and don’t maintain **`projection.run_id.values`**.

**Why not Glue ETL?**  
Transform logic is **Python** we already run in Lambda; no need for Spark/DPU for this scale.

**Why manual DDL?**  
**Catalog-as-code**: reproducible, matches pipeline paths, avoids crawler surprises.

**Why `latest_run_metadata`?**  
Cheap **pointer** for “current run” without scanning all partitions.

**Why `pipeline_run_summary`?**  
**Validation** row counts per run/month—proof the pipeline hung together.

**What is HHI?**  
Sum of squared **shares**—here applied to **skill-tag demand shares** as a concentration metric (see comparison view header for caveats).

**What is strict-common?**  
Months where **both** sources have comparable **latest run per month** rows—**fair** but can **drop** months.

**Why March was “missing”?**  
Usually **coverage/intersection** or **window**, not a single bug—check each source’s **`posted_month`** presence for that month.

**Why source-prefixed Silver/Gold?**  
Clear **multi-source** isolation and Athena **source** projection.

**Why comparison views vs physical tables?**  
**Iteration speed** and **no duplicate Parquet** for every logic tweak—trade-off is heavier SQL.

**Why cost anomaly?**  
Often **S3 request volume** (LIST/GET/sync/schedule)—mitigate with **schedule**, **sync excludes**, **fewer passes**.

**Why reduce schedule?**  
Capped **invocations** and downstream **writes/reads**.

**Skill extraction limits?**  
Rule-based list—not semantic understanding; good for MVP charts, not HR science.

**Overall limitations?**  
Two APIs, not global labor market; tag-level skill double-counting in some analytics; comparison window choices affect fairness.

**What would you improve next?**  
Automated **projection enum** update from pipeline output, **CI** on transforms, richer **geo** normalization, **monitoring** on Lambda failures.

---

## 22. Important file / folder map

| Area | Path | Notes |
|------|------|--------|
| **Pipelines** | `src/jmi/pipelines/ingest_live.py`, `ingest_adzuna.py`, `transform_silver.py`, `transform_gold.py` | Core batch logic |
| **Paths** | `src/jmi/paths.py` | Canonical S3 layout |
| **Skills** | `src/jmi/connectors/skill_extract.py` | Allowlist/rules |
| **Athena DDL** | `infra/aws/athena/ddl_gold_*.sql`, `ddl_silver_v2_*.sql`, `ddl_gold_latest_run_metadata_*.sql` | Projection-heavy |
| **Analytics v2 SQL** | `infra/aws/athena/analytics_v2_*.sql` | EU/India helpers |
| **Comparison** | `docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql`, `infra/aws/athena/comparison_v2_views.sql` | Views + obsolete table drops |
| **Deploy** | `scripts/deploy_athena_v2.py`, `deploy_athena_comparison_views_v2.py`, `deploy_jmi_analytics_v2_minimal.py`, `pipeline_live_sync.py` | Ops entry points |
| **AWS infra** | `infra/aws/lambda/`, `infra/aws/eventbridge/`, `infra/aws/iam/` | Lambda + schedule + roles |
| **Docs** | `docs/MIGRATION_V1_V2.md`, `docs/STORAGE_LAYOUT_MULTISOURCE.md`, `docs/aws_live_fix_gold_projection.md`, `docs/cost_guardrails.md`, `docs/dashboard_implementation/*` | Architecture + BI |
| **Archive** | `infra/aws/athena/archive_non_v2_ddl/` | Old Glue definitions |
| **Dashboard app** | `dashboard/app.py` | Local Streamlit |

---

## 23. Final study checklist

**Before demo / viva, understand**

- [ ] One full path: **API → Bronze → Silver → Gold → Athena → QS**
- [ ] **`run_id`** vs **`posted_month`** vs **`bronze_ingest_date`**
- [ ] **Partition projection** and why **`run_id` enum** must be updated after runs
- [ ] **Strict-common** vs **rolling window** in comparison SQL
- [ ] **Skill extraction** is **rules**, not ML

**Files to revise**

- [ ] `src/jmi/paths.py`, `transform_gold.py` (months/partitions)
- [ ] `infra/aws/athena/ddl_gold_skill_demand_monthly.sql` (projection)
- [ ] `ATHENA_VIEWS_COMPARISON_V2.sql` (intersection)
- [ ] `docs/aws_live_fix_gold_projection.md` (if teacher asks “no rows in Athena”)

**AWS pages / commands to remember**

- [ ] S3 bucket prefixes for **bronze/silver/gold**
- [ ] Athena workgroup + **s3://…/athena-results/**
- [ ] Glue **Table properties** → **`projection.run_id.values`**
- [ ] Lambda **chain**: ingest handler invokes Silver then Gold (async)

**Names to know**

- [ ] Databases: **`jmi_gold_v2`**, **`jmi_silver_v2`**, **`jmi_analytics_v2`**
- [ ] Gold facts: **`skill_demand_monthly`**, **`role_demand_monthly`**, **`location_demand_monthly`**, **`company_hiring_monthly`**, **`pipeline_run_summary`**
- [ ] Pointers: **`latest_run_metadata_arbeitnow`**, **`latest_run_metadata_adzuna`** — in **`jmi_gold_v2`** after deploy (`scripts/deploy_athena_v2.py` rewrites `jmi_gold.` → `jmi_gold_v2.` from `ddl_gold_latest_run_metadata_*.sql`; verify live Glue if unsure)
- [ ] Comparison views: **`comparison_source_skill_mix_aligned_top20`**, **`comparison_benchmark_aligned_month`** (see comparison SQL file for exact list)

---

## Document notes (maintenance)

- If **`docs/data_dictionary.md`** still says **`ingest_month`** for Gold in places, prefer **`posted_month`** as in **`ddl_gold_*.sql`** and **`transform_gold.py`**—update the dictionary when convenient.
- **QuickSight dashboard `dea final 9`:** Dataset list **not** stored in git—confirm against **QuickSight** before claiming exact visual bindings in an exam setting.
