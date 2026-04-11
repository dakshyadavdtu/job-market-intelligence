# QUICKSIGHT_BUILD_CHECKLIST.md

Step-by-step implementation manual for the frozen two-sheet dashboard. Follow order exactly unless a step fails (then use `VISUAL_FALLBACK_RULES.md`).

---

## A. Prep

### A1 — Athena

1. Open **Athena** (same workgroup/region as S3/Glue).
2. Apply **`infra/aws/athena/ddl_gold_*.sql`** (or `ALTER TABLE` the same `TBLPROPERTIES`) so fact tables use **partition projection**; keep **`pipeline_run_summary`** without projection (see comment in `ddl_gold_pipeline_run_summary.sql`).
3. Run **`ATHENA_VIEWS.sql`** end-to-end, then optional **`ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql`**.
4. Script uses `CREATE DATABASE IF NOT EXISTS jmi_analytics;` — if it fails, create the database manually in Athena, then re-run view statements.
5. After each Gold run that writes **new** `ingest_month` / `run_id` prefixes under `gold/pipeline_run_summary/`, register partitions for **that table only** (e.g. `MSCK REPAIR TABLE jmi_gold.pipeline_run_summary;` or a Glue Crawler on that prefix). Fact tables (`role_*`, `location_*`, `company_*`, `skill_*`) use partition projection and do **not** need MSCK for new runs, as long as `ingest_month` stays within the configured projection range.
6. Validate SQL (latest run is chosen automatically via `jmi_analytics.latest_pipeline_run`):
   - `SELECT run_id FROM jmi_analytics.latest_pipeline_run;` → newest `run_id` string.
   - `SELECT * FROM jmi_analytics.sheet1_kpis;` → one row per `ingest_month` in the **latest** pipeline run only.
   - `SELECT MAX(cumulative_job_pct) FROM jmi_analytics.role_pareto;` → **100.0** (within float tolerance).

### A2 — QuickSight account

1. Ensure QuickSight **same region** as Athena (or SPICE refresh supported path).
2. **Manage QuickSight** → **Security & permissions** → Athena + S3 access for gold bucket (if not already).

### A3 — Create datasets (Athena source)

Create **seven** datasets (names suggested; match `DASHBOARD_SPEC.md`):

| # | Dataset name | Athena table/view |
|---|----------------|-------------------|
| 1 | `DS_SHEET1_KPIS` | `jmi_analytics.sheet1_kpis` |
| 2 | `DS_SKILLS` | `jmi_analytics.skill_demand_monthly_latest` |
| 3 | `DS_LOC_TOP15` | `jmi_analytics.location_top15_other` |
| 4 | `DS_ROLE_PARETO` | `jmi_analytics.role_pareto` |
| 5 | `DS_ROLE_TOP20` | `jmi_analytics.role_top20` |
| 6 | `DS_COMPANY_TOP12` | `jmi_analytics.company_top12_other` |
| 7 | `DS_PIPELINE_SUMMARY` | `jmi_analytics.pipeline_run_summary_latest` |

For each dataset:

- Data source: **Athena**.
- Table/view: as above.
- **Import mode:** Direct Query **or** SPICE (if SPICE, schedule refresh after pipeline runs).
- Finish **without** analysis yet (or save default analysis — you will add visuals in dashboard).

### A4 — Dashboard parameters (optional)

`jmi_analytics` views already restrict data to **MAX(run_id)** from `jmi_gold.pipeline_run_summary` (see `latest_pipeline_run`). Parameters are **optional**: use **`p_ingest_month`** (and rarely **`p_run_id`**) only if you need to override or narrow a multi-month latest run in a visual.

---

## B. Build order: Sheet 2 first, then Sheet 1

**Why Sheet 2 first:** Single table + static text + one image; establishes **proof boundary** and confirms `DS_PIPELINE_SUMMARY` before spending time on Sheet 1 visuals. Reduces risk of duplicating proof content on Sheet 1 out of habit.

---

## C. Sheet 2 — Per-block build

### C1 — Create Sheet 2

1. New **analysis** → name e.g. `JMI_Final`.
2. Add **sheet** → rename to **Platform, pipeline & proof** (or frozen title from copy deck).

### C2 — S2-HDR (text)

1. Add visual → **Text box**.
2. Paste **`S2-HDR-TITLE`** and **`S2-HDR-SUBTITLE`** from `SHEET2_COPY_BLOCKS.md` (one or two text visuals).
3. **Check:** No market numbers.

### C3 — S2-LIFECYCLE (text)

1. Add **Text box** below header.
2. Paste **`S2-LIFECYCLE`** body.

### C4 — S2-ARCH-IMG (image)

1. Draw diagram per `ARCHITECTURE_DIAGRAM_BRIEF.md` → export **PNG**.
2. Add visual → **Image** → upload PNG.
3. **Check:** Image readable at dashboard width.

### C5 — S2-LAYER-CONTRACT (text)

1. Add **Text box**.
2. Paste **`S2-LAYER-CONTRACT`**.

### C6 — S2-PROOF-FRAMING (text, optional small block)

1. Add **Text box** one line above table (from `S2-PROOF-ABOVE-TABLE` in copy file if present).

### C7 — S2-PIPELINE-TABLE

1. Add visual → **Table**.
2. Dataset: **`DS_PIPELINE_SUMMARY`**.
3. **Fields:** drag `source`, `bronze_ingest_date`, `bronze_run_id`, `skill_row_count`, `role_row_count`, `location_row_count`, `company_row_count`, `status`, `ingest_month`, `run_id`.
4. **Filters:** Optional `ingest_month` if multiple months exist for the latest run; dataset is already limited to the latest pipeline run.
5. **Sort:** `ingest_month` ascending or `bronze_ingest_date` as needed.
6. **Formatting:** Wrap text off for numeric columns; align numbers right.
7. **Check:** `status` shows **PASS** for validated run; row counts match expectations.

### C8 — S2-SECURITY through S2-SWE (text blocks)

1. For each: add **Text block**, paste from `SHEET2_COPY_BLOCKS.md` in order:  
   **S2-SECURITY**, **S2-DATA-MGMT**, **S2-DATAOPS**, **S2-ORCHESTRATION**, **S2-SWE**.
2. **Check:** No charts sneaked in.

### C9 — Sheet 2 layout

1. Order top-down: HDR → Lifecycle → Image → Layer contract → Proof line → Pipeline table → Security → Data mgmt → DataOps → Orchestration → SWE.
2. **Spacing:** Consistent vertical gap (e.g. 16–24 px equivalent); section headings same font size.

---

## D. Sheet 1 — Per-visual build

### D1 — Create Sheet 1

1. Add sheet → rename **Market intelligence & structural evaluation**.

### D2 — Optional filters on Sheet 1 datasets

1. `jmi_analytics` datasets are **latest-run** by default. Add **`ingest_month`** filters only if a visual must show a **single** month while the latest run contains several months.
2. **Do not** filter Sheet 2 datasets with Sheet 1-only logic that hides the pipeline table.

**Common issue:** SPICE dataset shows stale data → **Refresh** dataset after pipeline runs (SPICE does not auto-pick up new Athena results).

### D3 — S1-HDR, S1-METRIC-DEF, S1-GUARDRAILS

1. **Five** copy blocks in order (combine into three text visuals as you prefer): `S1-HDR-TITLE`, `S1-HDR-SUBTITLE`, `S1-METRIC-DEF-BODY`, `S1-GUARDRAILS-TITLE`, `S1-GUARDRAILS-BODY` from `SHEET1_COPY_BLOCKS.md`.
2. **Check:** No `run_id` / `PASS` in text.

### D4 — S1-KPI-K1 … K6 (six KPIs)

1. Add **KPI** visual.
2. Dataset: **`DS_SHEET1_KPIS`**.
3. **Value field mapping:**
   - K1 → `total_postings`
   - K2 → `located_postings`
   - K3 → `top3_location_share` → set format **Percent** (0–1 vs 0–100 per QuickSight auto-detect — **verify** display: if raw is 0.42, show 42%).
   - K4 → `location_hhi` → **Decimal** (2–4 places).
   - K5 → `company_hhi` → **Decimal**.
   - K6 → `top1_role_share` → **Percent**.
4. Duplicate KPI five times or add six separate KPI visuals — align in **one row** (6 columns).
5. **Titles/subtitles:** copy from `DASHBOARD_SPEC.md` / `SHEET1_COPY_BLOCKS.md` per KPI.
6. **Check after each:** Values non-null for validated run; K2 ≤ K1.

**Critical (SPICE):** `DS_SHEET1_KPIS` has **one row per** `(ingest_month, run_id)` **within the latest pipeline run** only. If the latest run rebuilt **multiple** months, you still get **multiple** rows — use **`ingest_month`** filter on KPI visuals **or** aggregate in an analysis calculated field so QuickSight does not **Sum** KPI fields across months incorrectly.

**Common issue:** Percent shows 4200% → field is already 0–100; switch to decimal or divide in QS — **prefer** fix Athena view to output 0–1 for share fields only (current SQL: K3/K6 are 0–1).

### D5 — S1-DONUT-SKILLS

1. Add visual → **Donut chart**.
2. Dataset: **`DS_SKILLS`**.
3. **Angle:** `job_count`. **Color:** `skill`.
4. **Sort:** `job_count` descending.
5. **Filter:** Optional **`ingest_month`** if you need one month only (same latest `run_id` across rows).
6. **Data labels:** ON (percent or value per preference — prefer **value** + legend).
7. **Title/subtitle:** from spec.
8. **Check:** Exactly **7** slices (for current data); sum of labels ≠ total postings (do not display misleading “100% jobs”).

### D6 — S1-TREEMAP-LOC

1. Add **Treemap**.
2. Dataset: **`DS_LOC_TOP15`**.
3. **Group by:** `location_label`. **Size:** `job_count`.
4. **Filter:** Optional **`ingest_month`** (see D2).
5. **Tooltip:** `location_label`, `job_count`.
6. **Check:** One **Other** tile if long tail exists.

### D7 — S1-HIGHLIGHT-LOC

1. Add **Table**.
2. Same dataset **`DS_LOC_TOP15`**.
3. Columns: `location_label`, `job_count`.
4. **Sort:** `job_count` desc.
5. **Conditional formatting:** Data bars on `job_count` if available.
6. **Check:** Row count ≤ 16; sums to **located postings**.

### D8 — S1-PARETO-ROLE

1. Add **Combo chart** (bar + line).
2. Dataset: **`DS_ROLE_PARETO`**.
3. **X-axis:** `pareto_rank` (continuous or categorical — use **integer** ordering).
4. **Bar value:** `job_count`.
5. **Line value:** `cumulative_job_pct`.
6. **Sort:** `pareto_rank` ascending.
7. **Tooltip:** include **`role`**, `pareto_rank`, `job_count`, `cumulative_job_pct`, `share_of_total`.
8. **Check:** Last `cumulative_job_pct` = **100%**.

**Common issue:** Line flat or missing → line field on secondary axis; enable **dual axis** if QS requires.

### D9 — S1-TABLE-ROLE

1. Add **Table**.
2. Dataset: **`DS_ROLE_TOP20`**.
3. Columns: `pareto_rank`, `role`, `job_count`.
4. **Sort:** `pareto_rank` asc.
5. **Column widths:** Widen `role`; enable **wrap text**.
6. **Check:** 20 rows max.

### D10 — S1-TREEMAP-COMPANY

1. Add **Treemap**.
2. Dataset: **`DS_COMPANY_TOP12`**.
3. **Group:** `company_label`. **Size:** `job_count`.
4. **Check:** **Other** present if >12 companies.

If unreadable → **`VISUAL_FALLBACK_RULES.md`** Section Companies.

---

## E. Final layout pass (Sheet 1)

1. **Grid:** Top = text trio → KPI row (6) → Skills donut (narrow) + optional spacer → Locations (treemap + table side-by-side or stacked) → Pareto full width → Top 20 table full width → Company treemap.
2. **Heights:** Pareto **taller** than donut (information density).
3. **Theme:** One **color palette**; donut/treemap colors distinct enough.
4. **Avoid clutter:** No duplicate legends; hide `ingest_month`/`run_id` from visuals where only filter-driven.

---

## F. Publish & validation pass

1. **Publish** dashboard.
2. Run **`QA_VALIDATION_CHECKLIST.md`** in full.
3. **Share** with reviewer account if needed (permissions).
4. **Snapshot** PDF for viva backup (optional).

---

## G. If something breaks (quick routing)

| Symptom | Action |
|---------|--------|
| Empty KPIs | `latest_pipeline_run` NULL (repair **`pipeline_run_summary`** partitions); projection range; optional month filter |
| Pareto line wrong | Re-run Athena `role_pareto` query; check `total_jobs` |
| Percent wrong scale | Format KPI as percent vs decimal |
| Treemap illegible | Apply `VISUAL_FALLBACK_RULES.md` |
| Sheet 2 table empty | Pipeline summary path / partitions |
