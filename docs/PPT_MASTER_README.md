# PPT Master Plan for JMI Final Presentation

**Purpose:** Slide-by-slide, evidence-backed planning document for the **Job Market Intelligence (JMI)** final deck. Based on **active v2 / current repo state** (Bronze → Silver → Gold, `jmi_gold_v2` / `jmi_analytics_v2`, Arbeitnow + Adzuna India, Lambda chain, Athena views, QuickSight spec).

**How to use:** Build slides in order of §2 for a full story; use §8 for a short deck. Every slide lists **what to show**, **what to say**, and **strength** so you can cut weak content.

**Exact numbers:** Where this doc cites **repo-validated** figures (e.g. AWS checkpoint), they are copy-paste safe. **Live KPIs** (`total_postings`, HHI, etc.) must be pulled from Athena (`jmi_analytics.sheet1_kpis` / `jmi_analytics_v2.sheet1_kpis_adzuna`) or QuickSight at presentation time—those values change per run.

---

## 1. PPT strategy

- **Target audience:** Instructor / viva panel who can challenge **data semantics** (skills vs jobs, HHI meaning), **alignment** (EU vs India comparison), and **engineering choices** (why Bronze, why views, why not crawler-only).
- **PPT style:** Dark or high-contrast theme; **one insight per slide**; **visual-first** (diagram, table, KPI strip, screenshot); footnote with **artifact** (view name, `run_id`, path pattern)—not paragraphs of theory.
- **Story arc:** (1) Real-world mess of job feeds → (2) JMI’s contract (medallion + lineage) → (3) **Your** pipeline on AWS → (4) Silver cleaning with **named** rules → (5) Gold facts + Athena **semantic layer** → (6) Dashboard proof (Sheet 1 vs 2 boundary) → (7) EU vs India **strict-common** comparison → (8) Cost/trust/trade-offs → (9) What you proved + honest limits.
- **What to avoid:** Generic “Introduction / Methodology / Results” without **named objects** (`pipeline_run_summary`, `sheet1_kpis`, `comparison_benchmark_aligned_month`). Avoid implying **sum of skill `job_count` = total jobs** (explicitly forbidden in `METRIC_DEFINITIONS.md` / `DASHBOARD_SPEC.md`).

---

## 2. Recommended final slide sequence

**Legend:** (E) essential · (O) optional · **Strength:** strong / medium / weak

| # | Slide title (short) | E/O | Notes |
|---|---------------------|-----|--------|
| 1 | Title + one-line thesis | E | strong |
| 2 | Problem: why raw job feeds fail BI | E | strong |
| 3 | JMI outcome: what “Job Market Intelligence” means here | E | strong |
| 4 | Architecture flowchart (full project) | E | strong |
| 5 | Sources: Arbeitnow vs Adzuna India | E | strong |
| 6 | Bronze: immutable evidence layer | E | strong |
| 7 | Silver: cleaning matrix (source-specific) | E | strong |
| 8 | Silver: quality gates + artifacts | E | strong |
| 9 | Gold: five fact tables + run summary | E | strong |
| 10 | Storage layout v2 (`source=` partitions) | O | strong |
| 11 | Glue / Athena: catalog vs semantic views | E | strong |
| 12 | Dashboard Sheet 1 — market KPIs & charts | E | strong |
| 13 | Dashboard Sheet 2 — pipeline proof | E | strong |
| 14 | EU vs India: alignment + comparison views | E | strong |
| 15 | Lineage, determinism, `ingest_month` rule | O | medium |
| 16 | Cost & architecture decisions | E | medium |
| 17 | Issues, debugging, lessons | O | medium–strong |
| 18 | Conclusion: business value + limits + future | E | strong |

---

## 3. Detailed slide-by-slide plan

For each slide: **objective**, **exact content**, **data/evidence**, **visual**, **visual description**, **values**, **what to say**, **alternate**, **strength**.

### Slide 1 — Title: Job Market Intelligence (JMI)

1. **Objective:** Establish project identity and scope in one breath.
2. **Exact content:** Project name; **medallion data lake** (Bronze → Silver → Gold); **serverless AWS** target; **analytics** via Athena + QuickSight (and local Streamlit for dev).
3. **Data/evidence:** Repo title `README.md` §1; architecture one-liner.
4. **Best visual:** Title slide + **small stack diagram** (3 layers) or single **hero icon row** (API → S3 → Dashboard).
5. **Visual description:** Center title; bottom ribbon: **Arbeitnow** | **Adzuna India** | **S3** | **Lambda** | **Athena** | **QuickSight** with logos (fair use) or service icons.
6. **Exact values:** Optional subtitle: **two live source slugs** — `arbeitnow`, `adzuna_in` (`README.md` §6).
7. **What to say:** “JMI is not a single chart—it’s a **repeatable batch pipeline** that turns noisy APIs into **auditable** raw files, **standardized** job rows, and **small** aggregate tables for BI.”
8. **Alternate:** Add **your name / institution** only; no bullet wall.
9. **Strength:** **strong** — anchors everything else.

---

### Slide 2 — Problem: job-market data is not dashboard-ready

1. **Objective:** Justify Bronze/Silver/Gold **without** generic DE theory.
2. **Exact content:** Duplication; schema drift; semi-structured JSON; **skills as tags** (multi-valued) vs **job count**; need for **replay** and **audit**.
3. **Data/evidence:** `README.md` §2 Problem statement; `METRIC_DEFINITIONS.md` (“Skill job_count is not total jobs”).
4. **Best visual:** **Before/after** split panel — **Left:** “Raw API document” (fake mini JSON keys: `tags`, `location`, `company`). **Right:** “Silver row” fields: `job_id`, `title_norm`, `skills[]`, `bronze_run_id`.
5. **Visual description:** Annotate **one arrow** “Deterministic `job_id` + lineage” pointing to Silver.
6. **Exact values:** N/A (conceptual); optional: cite **quality checks** list from `run_silver_checks` (6 checks) — `src/jmi/utils/quality.py`.
7. **What to say:** “If we ‘clean in the connector,’ we **lose history**. If we chart raw JSON, every question is expensive and ambiguous. JMI **separates capture from meaning**.”
8. **Alternate:** **Risk table** — Row: Duplication / Drift / Cost; Column: Symptom → JMI layer that addresses it.
9. **Strength:** **strong** if tied to **skills vs jobs** rule; **weak** if generic.

---

### Slide 3 — Outcome: what JMI delivers

1. **Objective:** Define **deliverables**: lake layout, aggregates, SQL views, dashboard rules.
2. **Exact content:** MVP outcomes from `README.md` §3: micro-batch ingestion, partitioned S3, Glue + Athena, dashboard path (Streamlit now, QuickSight target); **v2** multi-source layout.
3. **Data/evidence:** `docs/STORAGE_LAYOUT_MULTISOURCE.md`; `docs/dashboard_implementation/DASHBOARD_SPEC.md` (two-sheet frozen dashboard).
4. **Best visual:** **Outcome hexagon** or **6-box grid**: Bronze audit | Silver quality | Gold speed | Athena SQL | QuickSight | Comparison (EU/IN).
5. **Visual description:** Each cell = **one artifact name** (e.g. `pipeline_run_summary`, `sheet1_kpis`).
6. **Exact values:** Dashboard: **Sheet 1** = market intel; **Sheet 2** = platform proof (`DASHBOARD_SPEC.md` §9–16).
7. **What to say:** “The product is **data + contracts**: files on S3, **views** in Athena, and a **frozen** dashboard spec so numbers are defensible.”
8. **Alternate:** Screenshot montage (tiny) of **Sheet 1 KPI strip** + **Sheet 2 pipeline table** with labels.
9. **Strength:** **strong**.

---

### Slide 4 (Option A) — Full architecture: swimlane flowchart

1. **Why it exists:** One slide answers “how does data move?” for viva.
2. **Exact content:** End-to-end path aligned with `docs/dashboard_implementation/ARCHITECTURE_DIAGRAM_BRIEF.md` **extended for v2**: add **Adzuna India** as second source; show **EventBridge → ingest Lambda**; show **`jmi_gold_v2` + `jmi_analytics_v2`**.
3. **Data/evidence:** Lambda chain: `ingest_handler.py` invokes Silver with **`InvocationType: Event`**; silver/gold handlers (same repo); `infra/aws/eventbridge/jmi-ingest-schedule.json`.
4. **Best visual:** **Left-to-right flowchart** (dark background, neon accents like your reference slides).
5. **Visual description — boxes (exact labels):**
   - **Sources:** `Arbeitnow API` · `Adzuna India API` (credentials `ADZUNA_APP_ID` / `ADZUNA_APP_KEY`)
   - **Trigger:** `Amazon EventBridge` — subtitle: `rate(24 hours)` (file: `jmi-ingest-schedule.json`; name `jmi-ingest-10min` is legacy naming—**call out** schedule is **24h** in talk)
   - **Compute:** `Lambda: jmi-ingest-live` → async invoke → `Lambda: jmi-transform-silver` → async invoke → `Lambda: jmi-transform-gold` (`infra/aws/lambda/README.md`)
   - **Storage:** `S3` — **Bronze** `…/bronze/source=<slug>/ingest_date=…/run_id=…/raw.jsonl.gz` + `manifest.json`; **Silver** `…/silver/jobs/source=<slug>/…/part-00001.parquet`; **Gold** `…/gold/<table>/source=<slug>/ingest_month=…/run_id=…/part-00001.parquet` (`STORAGE_LAYOUT_MULTISOURCE.md`)
   - **Pointers:** `gold/source=arbeitnow/latest_run_metadata/` · `gold/source=adzuna_in/latest_run_metadata/`
   - **Catalog & query:** `AWS Glue` → `Amazon Athena` — DBs **`jmi_gold` / `jmi_gold_v2`**, views **`jmi_analytics` / `jmi_analytics_v2`**
   - **BI:** `Amazon QuickSight` — cite frozen **two-sheet** spec
6. **Arrows / labels:** Ingest → Bronze `PUT`; Bronze path passed to Silver `invoke (Event)`; Silver → Gold `invoke (Event)`; Gold → Glue `DDL + partition projection`; Athena → QS `dataset (SPICE or DQ)`.
7. **How JMI differs from generic ETL:** **Async Lambda chain** (not Step Functions in MVP); **same Python modules** locally and in Lambda; **explicit DDL + projection** vs crawler-as-truth (`cost_guardrails.md`: avoid crawlers); **Gold-first** BI; **strict lineage** (`run_id`, `bronze_ingest_date`, `ingest_month` from lineage not wall clock—`README.md` §12).
8. **What to emphasize:** **Decoupling**: ingest completes Bronze before Silver; Silver passes **bronze file path** in payload (`ingest_handler.py` lines 19–26).
9. **Screenshot to include:** Optional small inset: **EventBridge schedule** snippet or **Lambda console** showing **container image** ECR deploy (`infra/aws/lambda/README.md`).
10. **Strength:** **strong** — if you verbally reconcile **schedule name vs 24h rate** to avoid confusion.

---

### Slide 4 (Option B) — Layered stack (Bronze / Silver / Gold / Serve)

1. **Visual:** Vertical **four-layer cake**; each layer lists **one example path fragment** from `STORAGE_LAYOUT_MULTISOURCE.md`.
2. **Insight:** Physical layout matches **logical contract** (audit → row grain → aggregate grain).
3. **Strength:** **strong** for memory; pair with **mini flowchart** arrow on the side.

---

### Slide 4 (Option C) — AWS service map + data flow

1. **Visual:** **Two zones**: “Edge” (APIs) vs “AWS account” (dashed box) with S3 + Lambda + Glue + Athena + QS inside.
2. **Use when:** Panel cares about **IAM / serverless** more than path details.

---

### Slide 5 — Data sources (active)

1. **Objective:** Prove **multi-source** is real, not slides-only.
2. **Exact content:**
   - **Arbeitnow:** `https://www.arbeitnow.com/api/job-board-api` — EU-focused public API (`README.md` §6).
   - **Adzuna India:** `source=adzuna_in` — India postings; env vars; runbook `docs/adzuna_india_runbook.md`.
3. **Data/evidence:** Connector paths `src/jmi/connectors/arbeitnow.py`; Adzuna pipeline `ingest_adzuna`.
4. **Best visual:** **Two-column table**: Source | **Partition slug** | **Format in Bronze** | **Scheduled ingest?**
5. **Exact values:** **Scheduled Lambda** runs **`ingest_live` (Arbeitnow) only** — Adzuna in image but **manual / separate entrypoint** unless you add another function (`infra/aws/lambda/README.md` “Scheduled jmi-ingest-live still invokes ingest_live (Arbeitnow) only”).
6. **What to say:** “We deliberately **namespace** sources so Gold and dashboards can **filter or compare** without silent mixing.”
7. **Insight:** Honesty about **automation scope** is a viva win.
8. **Strength:** **strong**.

---

### Slide 6 — Bronze layer

1. **Objective:** Show **immutable** raw capture + lineage fields.
2. **Exact content:** JSONL.gz; **`raw_payload` untouched**; **`run_id`**, **`bronze_ingest_date`**; `manifest.json`; health pointers `latest_ingest*.json` (`README.md` §8).
3. **Data/evidence:** Sample structure from `docs/data_dictionary.md` (open for field names).
4. **Best visual:** **Screenshot** of one **S3 console** prefix **or** tree diagram:
   - `bronze/source=arbeitnow/ingest_date=YYYY-MM-DD/run_id=<id>/raw.jsonl.gz`
5. **Visual description:** Callout box on **`raw_payload`**: “Full vendor JSON preserved.”
6. **Exact values:** Use a **real** `run_id` from your account when presenting; **repo-validated example:** `20260327T154416Z-fec115ef` (`docs/aws_checkpoint_analytics_validated_2026_03_27.md`) in **ap-south-1**, source `arbeitnow`.
7. **What to say:** “Bronze is our **evidence locker**—Silver rules can change; Bronze cannot lie about what the API returned.”
8. **Alternate:** One **fake redacted JSON line** (5–8 keys) + metadata columns.
9. **Strength:** **strong** with real path/`run_id`.

---

### Slide 7 — Silver layer (cleaning matrix)

1. **Objective:** Show **non-generic** Silver: mapping, skills, dedupe, contract enforcement.
2. **Exact content (from `README.md` §8 Silver):**
   - Flatten `raw_payload` → `title_norm`, `company_norm`, `location_raw`, `remote_type`, **`skills`** (allowlist / aliases / title & description; **Arbeitnow tag fallback**).
   - **Dedup** by `job_id`.
   - **`project_silver_to_contract`** drops legacy columns for Parquet schema match.
   - **`run_silver_checks`** → `silver_quality_*.json`.
   - Long text / URL / **`job_types`** stay on Bronze only (`data_dictionary.md`).
3. **Data/evidence:** `src/jmi/pipelines/transform_silver.py` (lineage from path `_extract_lineage_from_bronze_path`); quality metrics: `QualityReport` fields (`quality.py`).
4. **Best visual:** **Matrix table** (like your “dirty data” reference slide, but **real**):
   - Columns: **Source** | **Normalization** | **Dedup key** | **Quality artifact**
   - Rows: Arbeitnow | Adzuna_in
5. **Exact values:** Quality report includes **`duplicate_job_id`**, **`missing_title`**, **`missing_company`** counts (`quality.py`); **6** checks total.
6. **What to say:** “Silver is where **business rules live**—testable, repeatable, and **blocked** if checks fail.”
7. **Alternate (Slide 7B):** **Before/after** mini example: messy `location` string → `location_raw`; **skills array** capped/extracted (don’t fake percentages—use one real row from Parquet if allowed).
8. **Strength:** **strong** with one **real** example row or real quality JSON snippet.

---

### Slide 8 — Silver quality gates & lineage

1. **Objective:** Prove **trust** with artifacts, not adjectives.
2. **Exact content:** Output `data/quality/silver_quality_YYYY-MM-DD_<run_id>.json` (local path pattern from `README.md` §8).
3. **Best visual:** **Screenshot** of quality JSON **or** table: Check | Pass/Fail | Metric.
4. **Visual description:** Tie to **`run_silver_checks`** — row count > 0; no missing title/company; no duplicate `job_id` / duplicate source key (`quality.py` lines 54–60).
5. **Exact values:** Pull from **your** latest file; if presenting checkpoint run, align with same batch as Gold validation.
6. **What to say:** “Failed Silver **stops the story**—we don’t silently write bad Gold.”
7. **Strength:** **strong** with actual PASS.

---

### Slide 9 — Gold layer

1. **Objective:** Explain **five** aggregates + **pipeline_run_summary**.
2. **Exact content:** Tables (`README.md` §8 Gold):
   - `skill_demand_monthly`
   - `role_demand_monthly`
   - `location_demand_monthly`
   - `company_hiring_monthly`
   - `pipeline_run_summary` — **one row**: status + row counts
3. **Metric semantics:** **`ingest_month`** from **`bronze_ingest_date`** (first 7 chars), not wall clock at Gold time (`README.md` §12).
4. **Best visual:** **Five icons + one “proof” card** for `pipeline_run_summary`; show **column names** per table (`skill`, `role`, `location`, `company_name`, `job_count`).
5. **Exact values (repo checkpoint, one validated run):**  
   - `run_id`: **`20260327T154416Z-fec115ef`**  
   - `ingest_month`: **2026-03**  
   - Row counts: **skill_demand_monthly: 7**; **role_demand_monthly: 99**; **location_demand_monthly: 47**; **company_hiring_monthly: 66**; **pipeline_run_summary: 1** (status **PASS**) — source `docs/aws_checkpoint_analytics_validated_2026_03_27.md`.  
   - S3 bucket in doc: **`jmi-dakshyadav-job-market-intelligence`** (verify still current in your account).
6. **What to say:** “Gold answers **dashboard questions** in **small tables**—Athena scans **megabytes**, not full job history every time.”
7. **Alternate:** **Single table screenshot** from Athena `SELECT * FROM jmi_gold.pipeline_run_summary WHERE run_id = '…'`.
8. **Strength:** **strong** with **PASS** + row counts visible.

---

### Slide 10 (Optional) — v2 storage & rollback story

1. **Objective:** Show **modular** `source=` partitions and **safe migration** (`docs/MIGRATION_V1_V2.md`).
2. **Exact content:** v1 vs v2 table; **parallel** `jmi_gold` vs `jmi_gold_v2`; rollback by **switching DB/views**, not deleting data.
3. **Best visual:** **Side-by-side path strings** (legacy vs `source=arbeitnow`).
4. **Insight:** This is **production-grade** thinking—panel-ready.
5. **Strength:** **strong** for advanced audiences; **optional** if short on time.

---

### Slide 11 — Glue, Athena, analytics views

1. **Objective:** Defend **manual DDL + views** vs “just crawl it.”
2. **Exact content:**
   - Glue = **table definitions** + **partition projection** (see `docs/aws_live_fix_gold_projection.md` for **`run_id` values** maintenance).
   - Athena = **SQL** over S3.
   - **`jmi_analytics` / `jmi_analytics_v2`** = **semantic layer**: `sheet1_kpis`, `skill_demand_monthly_latest`, `role_group_pareto`, comparison views (`ATHENA_JMI_ANALYTICS_INVENTORY.md`).
3. **Best visual:** **Three-tier diagram**: Physical (S3) → Logical (Glue tables) → Semantic (views) → QuickSight.
4. **Exact artifact names:** Production datasets locked in `QUICKSIGHT_BUILD_CHECKLIST.md` §A3 (seven datasets — cross-check current file).
5. **What to say:** “Views encode **definitions** (KPI math, latest run, Top 15 + Other) so BI **doesn’t reimplement** fragile SQL in the UI.”
6. **Caution:** After each Gold run, **append new `run_id`** to projection (`aws_live_fix_gold_projection.md`) — good **debugging** slide material.
7. **Strength:** **strong**.

---

### Slide 12 — Dashboard Sheet 1 (market intelligence)

1. **Objective:** Show **frozen** analytics surface + **K1–K6**.
2. **Exact content:** `DASHBOARD_SPEC.md` Sheet 1 blocks: KPIs K1–K6, donut skills, treemap locations, role Pareto, company treemap; **no** `run_id` on Sheet 1 per frozen design preference.
3. **Data/evidence:** Views: `jmi_analytics.sheet1_kpis`, `skill_demand_monthly_latest`, `location_top15_other`, `role_group_pareto`, `company_top15_other_clean`.
4. **Best visual:** **Composite screenshot** of QuickSight Sheet 1 **or** rebuild key charts in PPT with **same** view names in footer.
5. **KPI definitions:** Use **`METRIC_DEFINITIONS.md`** for spoken definitions (total postings = sum of **role** `job_count`; located postings from **location** table; HHI formulas).
6. **Exact values:** **Fetch at slide-build time:**  
   `SELECT * FROM jmi_analytics.sheet1_kpis;`  
   (or `jmi_analytics_v2.sheet1_kpis` if that’s what your account uses). **Not** in repo as static numbers.
7. **What to say:** “Sheet 1 is **market structure**—concentration (HHI), geography mass, role families, skill **tag** composition. Skill chart is **not** additive to job totals.”
8. **What not to do:** Don’t sum skill `job_count` across skills as “jobs.”
9. **Strength:** **strong** with real KPI numbers filled in.

---

### Slide 13 — Dashboard Sheet 2 (platform & proof)

1. **Objective:** Separate **engineering proof** from **market story** (`DASHBOARD_SPEC.md` cross-sheet rule).
2. **Exact content:** `pipeline_run_summary_latest` table visual; architecture image **`S2-ARCH-IMG`** from `ARCHITECTURE_DIAGRAM_BRIEF.md`; text blocks: lifecycle, security, DataOps (`SHEET2_COPY_BLOCKS.md`).
3. **Best visual:** Split: **Left** small architecture PNG; **Right** table `source`, `bronze_run_id`, `skill_row_count`, … `status`.
4. **Exact values:** Use **`pipeline_run_summary`** row matching your Gold run (checkpoint shows **PASS** + per-dimension row counts).
5. **What to say:** “We **separated** concerns so reviewers can’t confuse **pipeline health** with **market performance**.”
6. **Strength:** **strong** — aligns with academic honesty.

---

### Slide 14 — Europe vs India comparison (v2 analytics)

1. **Objective:** Explain **fair comparison** + **view names** (not vibes).
2. **Exact content:** `ATHENA_VIEWS_COMPARISON_V2.sql`:
   - **`strict_intersection_latest_month`** — only months present **both** in EU and India **`role_demand_monthly`** within rolling window (`month_bounds`).
   - **`comparison_source_skill_mix_aligned_top20`** — top-20 skills by **combined** tag mass; shares **renormalized within top-20 per source**.
   - **`comparison_benchmark_aligned_month`** — role posting totals + **skill-tag HHI** (tag-mass semantics—not deduped per job).
3. **Best visual:** **Dual bar** (EU vs IN) for **one** aligned month from `comparison_benchmark_aligned_month` **or** **small multiples** for skill mix from `comparison_source_skill_mix_aligned_top20`.
4. **Exact values:** Pull from Athena for **your** latest deployed views—**not** hardcoded in repo. Optional: show **SQL fragment** on slide (short) as proof.
5. **What to say:** “We **don’t** compare arbitrary months—only **strict intersection**. Skill HHI here is on **tag distribution**, different from **location HHI** on **job shares** (`study/00_project_overview.md` HHI sections / `METRIC_DEFINITIONS.md`).”
6. **What not to do:** Map-based geo comparison **excluded** from Sheet 1 spec—don’t imply maps are in scope.
7. **Alternate:** Slide **14B** — `analytics_v2_cmp_location_hhi_monthly.sql` (monthly **location** HHI for intersection months) for **time-series** concentration story.
8. **Strength:** **strong** if you show **one** benchmark row + **alignment_kind** column interpretation; **weak** if only generic “EU vs India.”

---

### Slide 15 (Optional) — Lineage & determinism

1. **Objective:** Explain **`job_id`**, **`job_id_strategy`**, **`ingest_month`** discipline (`README.md` §12).
2. **Best visual:** **Single lineage chain** diagram: Ingest creates `run_id` → Bronze → Silver carries `bronze_data_file` → Gold partitions **`ingest_month`** from `bronze_ingest_date`.
3. **Strength:** **medium** — good if examiner asks reproducibility.

---

### Slide 16 — Cost & architecture trade-offs

1. **Objective:** Tie choices to **money + control**.
2. **Exact content:** `docs/cost_guardrails.md`: **≤ $3** total cap; serverless only; Parquet; partition pruning; Gold-first; **avoid crawlers**; manual DDL.
3. **EventBridge:** `rate(24 hours)` in `infra/aws/eventbridge/jmi-ingest-schedule.json` (adjust narrative if you changed it live).
4. **Best visual:** **Cost guardrail checklist** with **icons**; optional **tiny** table: Athena scan $/TB (public pricing—**look up current** AWS page at deck build time; **not** fixed in repo except qualitative guardrails).
5. **What to say:** “We optimized for **pay-per-use** and **query boundedness**—Gold and views exist so **QuickSight refresh** doesn’t mean ‘scan everything’.”
6. **Strength:** **medium** — stronger if you add **one** real bill line item screenshot (redacted).

---

### Slide 17 (Optional) — Issues & lessons

1. **Objective:** Show engineering maturity—**real** problems from docs.
2. **Candidate stories (pick 1–2):**
   - **Partition projection / `run_id` maintenance** after Gold runs (`docs/aws_live_fix_gold_projection.md`).
   - **v1→v2 coexistence** without destructive migration (`MIGRATION_V1_V2.md`).
   - **QuickSight dataset hygiene** — duplicate datasets risk (`ATHENA_JMI_ANALYTICS_INVENTORY.md` §QuickSight).
   - **Arbeitnow Silver path** still discovers **legacy flat** layout until migrated (`STORAGE_LAYOUT_MULTISOURCE.md` §Arbeitnow safety).
3. **Best visual:** **Before/After fix** one-pager: Symptom | Root cause | Fix | Artifact.
4. **Strength:** **strong** if specific; **weak** if vague “we had bugs.”

---

### Slide 18 — Conclusion

1. **Objective:** Close with **evidence-backed** claims + **limits**.
2. **Exact content:** Delivered: **multi-source** paths, **validation row** (`pipeline_run_summary`), **definable KPIs** (`sheet1_kpis`), **EU–IN comparison views** (`jmi_analytics_v2`). Limits: snapshot/not causal; employer names ≠ legal entities; skill tags **not** job counts; **strict-common** months may **drop** periods (`ATHENA_VIEWS_COMPARISON_V2.sql` header + `study/00_project_overview.md`).
3. **Best visual:** **Three bullets** + **one figure** (your best dashboard visual).
4. **What to say:** “JMI proves we can run a **credible** job-market analytics layer on **serverless** AWS with **auditable** batches and **SQL-defined** metrics.”
5. **Strength:** **strong**.

---

## 4. Full-project flowchart design (dedicated section)

Use this as the **single “hero” architecture slide** (combine with Slide 4 above).

### Boxes (recommended count: 10–12)

1. **Arbeitnow API** — HTTPS JSON job board.  
2. **Adzuna India API** — separate credentials; same Bronze envelope pattern.  
3. **Amazon EventBridge** — `rate(24 hours)` schedule → target **`jmi-ingest-live`**.  
4. **Lambda Ingest** — `jmi-ingest-live`; runs `ingest_live`; **async** invokes Silver.  
5. **S3 Bronze** — `raw.jsonl.gz`, `manifest.json`.  
6. **Lambda Silver** — `jmi-transform-silver`; reads Bronze path; writes Parquet; **async** invokes Gold.  
7. **S3 Silver** — `silver/jobs/source=<slug>/ingest_date=…/run_id=…/`.  
8. **Lambda Gold** — `jmi-transform-gold`; writes Gold + quality JSON + **`latest_run_metadata`** per source.  
9. **S3 Gold** — five fact folders + `pipeline_run_summary` + **`source=`** partitions (v2).  
10. **Glue Data Catalog** — `jmi_gold` / **`jmi_gold_v2`** + projection.  
11. **Athena** — SQL + **`jmi_analytics` / `jmi_analytics_v2`** views.  
12. **QuickSight** — Sheet 1 & 2 per `DASHBOARD_SPEC.md`.

### Arrows & labels

- API → Ingest: `GET / paginated fetch`  
- EventBridge → Ingest: `Invoke`  
- Ingest → Bronze: `PutObject`  
- Ingest → Silver Lambda: `invoke (Event)` + payload `bronze_file`, `run_id`  
- Silver → Silver S3: `Parquet`  
- Silver → Gold Lambda: `invoke (Event)`  
- Gold → Gold S3: `Parquet + JSON quality`  
- Gold S3 → Glue: `external table locations`  
- Glue → Athena: `metadata`  
- Athena → QuickSight: `datasets / views`

### Style

- **Color:** Compute (orange), Storage (green/gold tiers), Orchestration (pink/magenta), BI (purple)—match your reference deck.  
- **Emphasis:** **Async invokes** and **`source=` partitions** (this is your differentiator vs generic “ETL box”).  
- **Caption:** Adapt `ARCHITECTURE_DIAGRAM_BRIEF.md` “Figure” paragraph; add **v2** and **Adzuna**.

### Non-goals on this slide

- Do **not** put KPI numbers here—that’s Sheet 1. Optional small callout: **`pipeline_run_summary` PASS**.

---

## 5. Silver / data cleaning slide pack (dedicated)

### Cleaning categories (use as sub-slide or matrix rows)

| Category | JMI implementation | Evidence |
|----------|-------------------|----------|
| **Structural flattening** | `raw_payload` → typed columns | `transform_silver.py`, `data_dictionary.md` |
| **Skill extraction** | Allowlist + aliases + text + Arbeitnow tag fallback | `README.md` §8 |
| **Dedupe** | By `job_id`; deterministic id in connector | `README.md` §12, `quality.py` |
| **Schema contract** | `project_silver_to_contract` | `README.md` §8 |
| **Validation** | `run_silver_checks` | `quality.py` |
| **Lineage** | Path parsing → `bronze_run_id`, `bronze_ingest_date`, `bronze_data_file` | `transform_silver.py` `_extract_lineage_from_bronze_path` |

### Source-specific notes (viva-ready)

- **Arbeitnow:** legacy Silver path discovery may still apply until migration (`STORAGE_LAYOUT_MULTISOURCE.md`).  
- **Adzuna_in:** modular path **`silver/jobs/source=adzuna_in/...`** from day one.

### Before / after (example pattern, no fake stats)

- **Before:** nested JSON in `raw_payload`.  
- **After:** one row with **`skills` array** + **`title_norm`** — pull **one** real row from your Parquet for screenshot.

### Best layouts

- **Matrix** (fast).  
- **Two-column “Step A / Step B”** like your slides: **Step A** = normalize + extract; **Step B** = dedupe + QC (map to **Lambda Silver** only—don’t mix Athena SQL here).

---

## 6. Best dashboard visuals to include in PPT

| Visual (Sheet 1) | View | Why strong |
|------------------|------|------------|
| KPI strip K1–K6 | `sheet1_kpis` | Examiner can ask definitions—you have `METRIC_DEFINITIONS.md` |
| Skill donut | `skill_demand_monthly_latest` | Clear **composition** story if subtitled “tag mass, not job count” |
| Location treemap | `location_top15_other` | Shows **concentration** intuitively |
| Role Pareto combo | `role_group_pareto` | Head-vs-tail narrative |
| Company treemap | `company_top15_other_clean` | Employer concentration |

**Sheet 2:** `pipeline_run_summary_latest` table + architecture image — **mandatory** for “proof” narrative.

**Comparison (v2):** Benchmark row + optional aligned skill mix — **high insight** if explained with **strict intersection**.

---

## 7. Weak or risky visuals to avoid

| Visual / practice | Why risky | Mitigation |
|-------------------|-----------|------------|
| Summing skill `job_count` to “total jobs” | **Wrong** per `METRIC_DEFINITIONS.md` | Always show **K1 from `sheet1_kpis`** for totals |
| Maps (geo) | **Explicitly excluded** from Sheet 1 frozen spec | Don’t imply delivery |
| Legacy `company_top12_other` alongside `company_top15_other_clean` | Confusing duplicate grain | Use **one** clean view only |
| `comparison_region_*` multilayer helpers | Marked optional/legacy in v2 strategy (`QUICKSIGHT_V2_DATASET_STRATEGY.md`) | Prefer **`comparison_*_v2`** views |
| HHI without stating **which mass** (location vs skill-tag) | Easy viva trap | Label: “Location HHI (job shares)” vs “Skill-tag HHI (tag shares)” |
| Pretty chart with no **view name** / **run_id** | Looks decorative | Footer: view + `run_id` or month |

---

## 8. Final recommended deck (limited slide count)

**12 slides (tight, high grade):**

1. Title  
2. Problem (feeds vs BI)  
3. Architecture flowchart (§4)  
4. Bronze + Silver matrix  
5. Gold + **`pipeline_run_summary`** numbers (**use real** `run_id`)  
6. Glue/Athena/views  
7. Sheet 1 screenshot (KPIs + 1 chart)  
8. Sheet 2 screenshot (proof table + arch)  
9. EU vs India comparison (benchmark row + **one** insight)  
10. Cost & guardrails ($3 cap, serverless, Gold-first)  
11. One **lesson learned** (projection / migration / async chain)  
12. Conclusion + limits  

**If you only add one “wow” number:** Use **`pipeline_run_summary`** row counts + **PASS** from your latest validated batch (template values available in `docs/aws_checkpoint_analytics_validated_2026_03_27.md`).

---

## Appendix: Quick evidence lookup table

| Need | Where |
|------|--------|
| KPI math | `docs/dashboard_implementation/METRIC_DEFINITIONS.md` |
| Dashboard layout | `docs/dashboard_implementation/DASHBOARD_SPEC.md` |
| Athena view names | `docs/dashboard_implementation/ATHENA_JMI_ANALYTICS_INVENTORY.md` |
| Comparison SQL | `docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql` |
| S3 paths v2 | `docs/STORAGE_LAYOUT_MULTISOURCE.md` |
| Lambda deploy | `infra/aws/lambda/README.md` |
| Schedule | `infra/aws/eventbridge/jmi-ingest-schedule.json` |
| Validated run example | `docs/aws_checkpoint_analytics_validated_2026_03_27.md` |
| HHI deep explanation | `study/00_project_overview.md` (HHI sections) |

---

*Generated from repository state: Job Market Intelligence — active v2 paths, dual sources (Arbeitnow + Adzuna India), Lambda async chain, Athena analytics views, frozen QuickSight two-sheet spec.*
