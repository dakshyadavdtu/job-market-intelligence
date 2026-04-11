# QA_VALIDATION_CHECKLIST.md

Rigorous pre-submission QA for the frozen two-sheet dashboard. Check **every** box before viva.

**Validated reference slice (example):** `ingest_month = '2026-03'`, `run_id = '20260327T154416Z-fec115ef'` — replace if your run differs.

---

## 1. Data reconciliation (Athena)

| # | Check | How | Pass criterion |
|---|--------|-----|----------------|
| 1.1 | Total postings | `SELECT SUM(job_count) FROM jmi_gold.role_demand_monthly WHERE ingest_month='2026-03' AND run_id='...'` | Equals `total_postings` in `jmi_analytics.sheet1_kpis` for same slice. |
| 1.2 | Located postings | `SELECT SUM(job_count) FROM jmi_gold.location_demand_monthly WHERE ...` | Equals `located_postings` in `sheet1_kpis`; **≤** total postings. |
| 1.3 | Skill sum ≠ jobs | `SELECT SUM(job_count) FROM jmi_gold.skill_demand_monthly WHERE ...` | **Do not** compare to total postings as equality; document inequality is OK. |
| 1.4 | Location top15+Other sum | `SELECT SUM(job_count) FROM jmi_analytics.location_top15_other WHERE ...` | Equals located postings (1.2). |
| 1.5 | Company top12+Other sum | `SELECT SUM(job_count) FROM jmi_analytics.company_top12_other WHERE ...` | Equals sum of `job_count` in raw `company_hiring_monthly` for slice. |
| 1.6 | Role pareto endpoint | `SELECT MAX(cumulative_job_pct), MIN(pareto_rank), MAX(pareto_rank) FROM jmi_analytics.role_pareto WHERE ...` | `MAX(cumulative_job_pct)` ∈ [99.99, 100.01] (float tolerance); `MAX(pareto_rank)` = row count of `role_demand_monthly`. |
| 1.7 | Role pareto share sum | `SELECT SUM(share_of_total) FROM jmi_analytics.role_pareto WHERE ...` | ≈ **1.0** (tolerance 0.001). |
| 1.8 | Top20 subset | `SELECT COUNT(*) FROM jmi_analytics.role_top20 WHERE ...` | ≤ **20**; `pareto_rank` 1–20 consistent with `role_pareto` for same roles. |

---

## 2. KPI math (spot-check in Athena)

| # | Check | Pass criterion |
|---|--------|----------------|
| 2.1 | Top-3 location share | Manual: sum top 3 `job_count` from `location_demand_monthly` / sum all location `job_count` | Matches `top3_location_share` in `sheet1_kpis`. |
| 2.2 | Location HHI | Manual HHI from location shares | Matches `location_hhi` within tolerance **0.0001** or exact. |
| 2.3 | Company HHI | Same for companies | Matches `company_hhi`. |
| 2.4 | Top-1 role share | `MAX(job_count)/SUM(job_count)` on `role_demand_monthly` | Matches `top1_role_share`. |

---

## 3. Proof boundary (Sheets 1 vs 2)

| # | Check | Pass criterion |
|---|--------|----------------|
| 3.1 | No `pipeline_run_summary` on Sheet 1 | Visual inventory | **Zero** visuals using `DS_PIPELINE_SUMMARY` on Sheet 1. |
| 3.2 | No PASS badge / run proof on Sheet 1 | Scan Sheet 1 | No `status`, no `PASS`, no `pipeline_run_summary` table. |
| 3.3 | No `run_id` on Sheet 1 (strict mode) | Scan text/KPI | No `run_id` string in titles/subtitles; **if** tooltips show `run_id`, remove for strict freeze. |
| 3.4 | No market charts on Sheet 2 | Visual inventory | No donut, treemap, pareto, skill/role/loc/company charts. |
| 3.5 | Sheet 2 has pipeline table | Visual inventory | **`DS_PIPELINE_SUMMARY`** table present with expected fields. |

---

## 4. Filter behavior

| # | Check | Pass criterion |
|---|--------|----------------|
| 4.1 | Changing `ingest_month` / `run_id` | Apply filter | All Sheet 1 visuals update consistently; no blank KPIs if data exists in Athena. |
| 4.2 | Sheet 2 pipeline table | Same filters | Shows matching run or **intentionally fixed** parameter — **document** if Sheet 2 uses independent default. |
| 4.3 | No accidental cross-filter | Click interactions | Sheet 1 selections do not **break** Sheet 2 if unsupported (disable cross-sheet filtering if needed). |

---

## 5. Visual sanity

| # | Check | Pass criterion |
|---|--------|----------------|
| 5.1 | Donut | 7 slices (current data) | Legend matches `skill` values. |
| 5.2 | Location treemap | ≤ **16** tiles | Includes **Other** when tail exists. |
| 5.3 | Company treemap | ≤ **13** tiles | Top 12 + Other. |
| 5.4 | Pareto | X = `pareto_rank` ascending | Line ends at **100%**. |
| 5.5 | Top 20 table | Row count | **≤ 20**; sorted by `pareto_rank`. |

---

## 6. Tooltip & label readability

| # | Check | Pass criterion |
|---|--------|----------------|
| 6.1 | Pareto tooltip | Hover mid-`pareto_rank` | Shows **`role`**, `pareto_rank`, `job_count`, `cumulative_job_pct`, `share_of_total` (per `DASHBOARD_SPEC.md` S1-PARETO-ROLE). |
| 6.2 | Long company names | Treemap or table | No unreadable overlap; fallback applied per `VISUAL_FALLBACK_RULES.md` if needed. |
| 6.3 | KPI percent | K3, K6 | Display as **percent**, not raw 0.42 mislabeled as 42×. |

---

## 7. Copy correctness

| # | Check | Pass criterion |
|---|--------|----------------|
| 7.1 | Sheet 1 guardrails | Read aloud | No trend claims; no causality. |
| 7.2 | Sheet 1 | No Bronze/Silver/Gold **implementation** paragraphs | Only metric definitions allowed. |
| 7.3 | Sheet 2 | No market “insight” language | No HHI/location interpretation on Sheet 2. |
| 7.4 | `skill_row_count` | In pipeline table | Not described as “jobs” in adjacent text. |

---

## 8. Architecture / proof consistency

| # | Check | Pass criterion |
|---|--------|----------------|
| 8.1 | Diagram vs narrative | Compare image to `S2-LIFECYCLE` | Same flow: API → S3 Bronze → Silver → Gold → Athena → QuickSight. |
| 8.2 | Pipeline table row | For selected `run_id` | `status` = **PASS** for validated run; counts positive integers. |
| 8.3 | Layer contract | `S2-LAYER-CONTRACT` | Matches actual repo stages (Bronze raw, Silver job, Gold aggregate). |

---

## 9. Final sign-off

| # | Sign-off |
|---|----------|
| ☐ | All Athena views execute without error |
| ☐ | All QuickSight datasets refresh / query successfully |
| ☐ | Sheet 1 / Sheet 2 boundary checks passed |
| ☐ | Pareto cumulative ends at 100% |
| ☐ | Located postings ≤ total postings |
| ☐ | Screenshot or PDF export stored for submission |
