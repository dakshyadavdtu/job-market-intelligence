# DASHBOARD_SPEC.md

## Final frozen dashboard specification

This document is the **single master reference** for the QuickSight dashboard. The design is **locked**: two sheets only, roles and content boundaries as specified below. Do not add visuals, datasets, or proof elements outside this spec without a formal scope change.

---

## Sheet separation (frozen)

| Sheet | Name | Purpose |
|-------|------|---------|
| **Sheet 1** | Market intelligence & structural evaluation | Structural/evaluative analytics from **four demand gold datasets** + **Athena views** derived only from those tables. |
| **Sheet 2** | Platform, pipeline & proof | AWS lifecycle, architecture, controls, and **`pipeline_run_summary`** validation evidence. **No** market charts. |

**Cross-sheet rule:** No `pipeline_run_summary`, `run_id` proof detail, `PASS`, or Bronze/Silver/Gold **implementation** narrative on Sheet 1. No skill/role/location/company visuals on Sheet 2.

---

## Sheet 1 — Block-by-block specification

### S1-HDR — Header title + scope

| Attribute | Value |
|-----------|--------|
| **Purpose** | Establish page identity and honest analytic scope (single monthly snapshot). |
| **Sheet** | 1 |
| **Dataset / view** | None (static text). |
| **Visual type** | Text |
| **Fields used** | N/A |
| **Sorting** | N/A |
| **Filters** | None on text. |
| **Tooltip** | N/A |
| **Title** | *(Use `SHEET1_COPY_BLOCKS.md` — `S1-HDR-TITLE`)* |
| **Subtitle** | *(Use `SHEET1_COPY_BLOCKS.md` — `S1-HDR-SUBTITLE`)* |
| **Why this exists** | Frames interpretation; prevents trend/forecast misread. |
| **What NOT to show** | `run_id`, `PASS`, pipeline row counts, AWS service names in proof style. |

---

### S1-METRIC-DEF — Metric definitions strip

| Attribute | Value |
|-----------|--------|
| **Purpose** | Define K1–K6 and key denominators so KPIs are examinable. |
| **Sheet** | 1 |
| **Dataset / view** | None (static text). |
| **Visual type** | Text |
| **Fields used** | N/A |
| **Sorting** | N/A |
| **Filters** | None. |
| **Tooltip** | N/A |
| **Title** | *(optional small heading: “How to read the metrics”)* |
| **Subtitle** | *(Use `SHEET1_COPY_BLOCKS.md` — `S1-METRIC-DEF-BODY`)* |
| **Why this exists** | Academic defensibility; separates located vs total postings. |
| **What NOT to show** | Full SQL; engineering proof. |

---

### S1-GUARDRAILS — Interpretation guardrails

| Attribute | Value |
|-----------|--------|
| **Purpose** | State limitations (snapshot, non-causal, skill semantics). |
| **Sheet** | 1 |
| **Dataset / view** | None. |
| **Visual type** | Text |
| **Title** | *(Use copy block `S1-GUARDRAILS-TITLE`)* |
| **Body** | *(Use `S1-GUARDRAILS-BODY`)* |
| **What NOT to show** | Pipeline validation language belonging on Sheet 2. |

---

### S1-KPI-K1 — Total postings

| Attribute | Value |
|-----------|--------|
| **Purpose** | Canonical job/posting count for the slice. |
| **Sheet** | 1 |
| **Dataset / view** | `jmi_analytics.sheet1_kpis` (or equivalent path after deploy) |
| **Visual type** | KPI |
| **Fields used** | `total_postings` (value) |
| **Sorting** | N/A |
| **Filters** | Dashboard filter: `ingest_month`, `run_id` (must match `sheet1_kpis` row). |
| **Tooltip** | Optional: show `ingest_month`, `run_id` only if you accept minimal lineage on Sheet 1; **frozen design prefers no run_id on Sheet 1** — omit from tooltip or show ingest_month only. |
| **Title** | Total postings |
| **Subtitle** | Sum of job counts by role (canonical universe). |
| **Why** | Anchor all share math; matches `role_demand_monthly` semantics. |
| **What NOT to show** | Skill sum as jobs. |

---

### S1-KPI-K2 — Located postings

| Attribute | Value |
|-----------|--------|
| **Purpose** | Count postings with a non-empty normalized location. |
| **Sheet** | 1 |
| **Dataset / view** | `jmi_analytics.sheet1_kpis` |
| **Visual type** | KPI |
| **Fields used** | `located_postings` |
| **Filters** | Same as K1. |
| **Title** | Located postings |
| **Subtitle** | Sum of location table job counts (≤ total postings). |
| **What NOT to show** | Claim that this equals total postings unless equal. |

---

### S1-KPI-K3 — Top-3 location share (located mass)

| Attribute | Value |
|-----------|--------|
| **Purpose** | Concentration of located postings in top 3 locations. |
| **Dataset / view** | `jmi_analytics.sheet1_kpis` |
| **Visual type** | KPI |
| **Fields used** | `top3_location_share` (display as **percent**, 0–100 or 0–1 per QS format) |
| **Filters** | Same as K1. |
| **Title** | Top-3 location share |
| **Subtitle** | Share of located postings in the three largest location buckets. |
| **What NOT to show** | Denominator = total postings (wrong); must be located mass. |

---

### S1-KPI-K4 — Location HHI

| Attribute | Value |
|-----------|--------|
| **Purpose** | Concentration index across locations (located mass). |
| **Dataset / view** | `jmi_analytics.sheet1_kpis` |
| **Fields used** | `location_hhi` |
| **Filters** | Same as K1. |
| **Title** | Location HHI |
| **Subtitle** | Herfindahl index over location posting shares. |
| **What NOT to show** | Interpretation as “market power” in antitrust sense; this is descriptive concentration. |

---

### S1-KPI-K5 — Company HHI

| Attribute | Value |
|-----------|--------|
| **Purpose** | Concentration of postings across employer names. |
| **Dataset / view** | `jmi_analytics.sheet1_kpis` |
| **Fields used** | `company_hhi` |
| **Filters** | Same as K1. |
| **Title** | Company HHI |
| **Subtitle** | Herfindahl index over company posting shares. |
| **What NOT to show** | Confusion with location HHI — label clearly. |

---

### S1-KPI-K6 — Top-1 role share

| Attribute | Value |
|-----------|--------|
| **Purpose** | Dominance of single largest title bucket. |
| **Dataset / view** | `jmi_analytics.sheet1_kpis` |
| **Fields used** | `top1_role_share` |
| **Filters** | Same as K1. |
| **Title** | Top-1 role share |
| **Subtitle** | Largest role bucket ÷ total postings. |
| **What NOT to show** | Skill-based denominator. |

---

### S1-DONUT-SKILLS — Skills composition

| Attribute | Value |
|-----------|--------|
| **Purpose** | Part-to-whole view of skill-tag demand (not additive to job count). |
| **Sheet** | 1 |
| **Dataset / view** | `jmi_gold.skill_demand_monthly` |
| **Visual type** | Donut chart |
| **Fields used** | **Angle / Size:** `job_count`; **Color:** `skill` |
| **Sorting** | By `job_count` descending (or alphabetically by skill — prefer **job_count desc**). |
| **Filters** | `ingest_month`, `run_id` |
| **Tooltip fields** | `skill`, `job_count`; optional: `% of skill tag total` = `job_count / SUM(job_count)` within filtered slice (**not** % of jobs). |
| **Title** | Skill tag composition |
| **Subtitle** | Share of tag counts; jobs can carry multiple tags — do not sum to total postings. |
| **Why** | Seven short labels suit donut; composition story. |
| **What NOT to show** | Implied “total jobs” = sum of slice `job_count` on skills. |

---

### S1-TREEMAP-LOC — Locations treemap

| Attribute | Value |
|-----------|--------|
| **Purpose** | Show mass inequality across locations (Top 15 + Other). |
| **Sheet** | 1 |
| **Dataset / view** | `jmi_analytics.location_top15_other` |
| **Visual type** | Treemap |
| **Fields used** | **Group by:** `location_label` (or column name emitted by view — use `location_label`); **Size:** `job_count` |
| **Sorting** | By `job_count` desc (treemap often auto-sizes). |
| **Filters** | `ingest_month`, `run_id` |
| **Tooltip** | `location_label`, `job_count`, optional share of located mass. |
| **Title** | Geographic mass (Top 15 + Other) |
| **Subtitle** | Area ∝ located postings in each bucket. |
| **What NOT to show** | Raw map geo; more than 16 boxes without Other aggregation. |

---

### S1-HIGHLIGHT-LOC — Locations highlight table

| Attribute | Value |
|-----------|--------|
| **Purpose** | Readable tabular backup for location buckets. |
| **Sheet** | 1 |
| **Dataset / view** | `jmi_analytics.location_top15_other` (same as treemap) |
| **Visual type** | Table (Highlight table / conditional formatting if available) |
| **Fields used** | Columns: `location_label`, `job_count`; optional `ingest_month`, `run_id` hidden or in tooltip only. |
| **Sorting** | `job_count` descending |
| **Filters** | `ingest_month`, `run_id` |
| **Tooltip** | Same row fields. |
| **Title** | Location buckets (detail) |
| **Subtitle** | Same data as treemap; sortable list. |
| **What NOT to show** | Different N than 15+Other (must match view). |

---

### S1-PARETO-ROLE — Roles Pareto combo

| Attribute | Value |
|-----------|--------|
| **Purpose** | Head vs tail: cumulative posting coverage by ranked role. |
| **Sheet** | 1 |
| **Dataset / view** | `jmi_analytics.role_pareto` |
| **Visual type** | Combo chart (bar + line) |
| **Fields used** | **X-axis:** `pareto_rank` (integer 1…R); **Bar:** `job_count`; **Line:** `cumulative_job_pct` |
| **Sorting** | `pareto_rank` ascending |
| **Filters** | `ingest_month`, `run_id` |
| **Tooltip** | **`role`**, `pareto_rank`, `job_count`, `cumulative_job_pct`, `share_of_total` |
| **Title** | Role titles — Pareto coverage |
| **Subtitle** | Bars = postings per title; line = cumulative % of total postings. |
| **What NOT to show** | Truncated ranks for line endpoint (line must reach 100% at max rank). |

---

### S1-TABLE-ROLE — Top 20 roles table

| Attribute | Value |
|-----------|--------|
| **Purpose** | Readable long labels for top titles. |
| **Sheet** | 1 |
| **Dataset / view** | `jmi_analytics.role_top20` |
| **Visual type** | Table |
| **Fields used** | `role`, `job_count`, `pareto_rank`; hide `ingest_month`/`run_id` or use as filter only. |
| **Sorting** | `pareto_rank` ascending |
| **Filters** | `ingest_month`, `run_id` |
| **Tooltip** | N/A (table) |
| **Title** | Top 20 role titles (by postings) |
| **Subtitle** | Full title text; ties broken by view logic. |
| **What NOT to show** | All 99 rows; different ranking than Pareto base order (must both sort by job_count desc). |

---

### S1-TREEMAP-COMPANY — Companies treemap

| Attribute | Value |
|-----------|--------|
| **Purpose** | Employer mass concentration (Top 12 + Other). |
| **Sheet** | 1 |
| **Dataset / view** | `jmi_analytics.company_top12_other` |
| **Visual type** | Treemap |
| **Fields used** | **Group:** `company_label`; **Size:** `job_count` |
| **Sorting** | N/A (size-driven) |
| **Filters** | `ingest_month`, `run_id` |
| **Tooltip** | `company_label`, `job_count` |
| **Title** | Employer mass (Top 12 + Other) |
| **Subtitle** | Area ∝ postings per employer bucket. |
| **What NOT to show** | 66 separate tiles without Other. |

---

## Sheet 2 — Block-by-block specification

### S2-HDR — Engineering header + page purpose

| Attribute | Value |
|-----------|--------|
| **Purpose** | Identify Sheet 2 as engineering proof, not analytics. |
| **Sheet** | 2 |
| **Dataset** | None |
| **Visual type** | Text |
| **Content** | `SHEET2_COPY_BLOCKS.md` — `S2-HDR-TITLE`, `S2-HDR-SUBTITLE` |

---

### S2-LIFECYCLE — End-to-end lifecycle narrative

| Attribute | Value |
|-----------|--------|
| **Purpose** | Source → ingestion → storage → transform → serve → consume. |
| **Sheet** | 2 |
| **Visual type** | Text |
| **Content** | `S2-LIFECYCLE` |

---

### S2-ARCH-IMG — Architecture diagram

| Attribute | Value |
|-----------|--------|
| **Purpose** | One-glance AWS flow. |
| **Sheet** | 2 |
| **Dataset** | None |
| **Visual type** | Image (upload PNG/SVG) |
| **Source** | Build from `ARCHITECTURE_DIAGRAM_BRIEF.md` |
| **What NOT to show** | Market KPIs; chart thumbnails. |

---

### S2-LAYER-CONTRACT — Bronze / Silver / Gold

| Attribute | Value |
|-----------|--------|
| **Purpose** | Define layer grain and intent for this project. |
| **Sheet** | 2 |
| **Visual type** | Text |
| **Content** | `S2-LAYER-CONTRACT` |

---

### S2-PROOF-ABOVE-TABLE — Proof framing (text above validation table)

| Attribute | Value |
|-----------|--------|
| **Purpose** | One short bridge explaining that the following table is pipeline validation evidence. |
| **Sheet** | 2 |
| **Dataset** | None |
| **Visual type** | Text |
| **Content** | `SHEET2_COPY_BLOCKS.md` — `S2-PROOF-ABOVE-TABLE` |

---

### S2-PIPELINE-TABLE — pipeline_run_summary evidence

| Attribute | Value |
|-----------|--------|
| **Purpose** | Validation artifact: PASS, counts, lineage keys. |
| **Sheet** | 2 |
| **Dataset** | `jmi_gold.pipeline_run_summary` |
| **Visual type** | Table (pivot not required) |
| **Fields used** | `source`, `bronze_ingest_date`, `bronze_run_id`, `skill_row_count`, `role_row_count`, `location_row_count`, `company_row_count`, `status`, `ingest_month`, `run_id` |
| **Sorting** | `run_id` desc or single row — typically one row per filter. |
| **Filters** | Optional: `ingest_month`, `run_id` to select validated run |
| **Title** | Pipeline run summary (validation) |
| **Subtitle** | Gold-stage quality snapshot for this run. |
| **What NOT to show** | Interpretation of skill_row_count as jobs. |

---

### S2-SECURITY — Security text

| Attribute | Value |
|-----------|--------|
| **Sheet** | 2 |
| **Visual type** | Text |
| **Content** | `S2-SECURITY` |

---

### S2-DATA-MGMT — Data management text

| Attribute | Value |
|-----------|--------|
| **Sheet** | 2 |
| **Visual type** | Text |
| **Content** | `S2-DATA-MGMT` |

---

### S2-DATAOPS — DataOps / reliability text

| Attribute | Value |
|-----------|--------|
| **Sheet** | 2 |
| **Visual type** | Text |
| **Content** | `S2-DATAOPS` |

---

### S2-ORCHESTRATION — Orchestration text

| Attribute | Value |
|-----------|--------|
| **Sheet** | 2 |
| **Visual type** | Text |
| **Content** | `S2-ORCHESTRATION` |

---

### S2-SWE — Software engineering text

| Attribute | Value |
|-----------|--------|
| **Sheet** | 2 |
| **Visual type** | Text |
| **Content** | `S2-SWE` |

---

## What is intentionally excluded

| Excluded | Reason |
|----------|--------|
| Third sheet | Frozen scope. |
| `pipeline_run_summary` on Sheet 1 | Proof belongs on Sheet 2. |
| Maps on Sheet 1 | Frozen: map fully excluded. |
| Market charts on Sheet 2 | Frozen boundary. |
| Skill sum as total jobs | Violates canonical rule. |
| Additional KPIs beyond K1–K6 | Frozen KPI strip. |
| Other bucket on skills | Seven skills only. |
| Pareto truncation | Full rank set in `role_pareto`. |
| Trend/time-series visuals | Single-month snapshot; no multi-month data in spec. |

---

## Dataset inventory (QuickSight)

| Logical name | Athena source |
|----------------|---------------|
| DS_SHEET1_KPIS | `jmi_analytics.sheet1_kpis` |
| DS_SKILLS | `jmi_gold.skill_demand_monthly` |
| DS_LOC_TOP15 | `jmi_analytics.location_top15_other` |
| DS_ROLE_PARETO | `jmi_analytics.role_pareto` |
| DS_ROLE_TOP20 | `jmi_analytics.role_top20` |
| DS_COMPANY_TOP12 | `jmi_analytics.company_top12_other` |
| DS_PIPELINE_SUMMARY | `jmi_gold.pipeline_run_summary` |

Apply **dashboard-level filters** on `ingest_month` and `run_id` where the underlying table exposes partition columns.
