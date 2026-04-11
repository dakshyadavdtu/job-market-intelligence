# VISUAL_FALLBACK_RULES.md

Frozen fallbacks when QuickSight rendering is poor. **Do not** default everything to horizontal bars.

**Story vs presentation:** All fallbacks here preserve **the same aggregates and metrics** — only **visual encoding** changes.

---

## S1-DONUT-SKILLS — Donut chart

| Item | Rule |
|------|------|
| **Trigger** | Labels overlap; seven slices too thin; legend unreadable at dashboard size. |
| **Fallback visual** | **Horizontal bar chart** — **Y:** `skill`, **X:** `job_count`, **sort:** `job_count` desc. |
| **Stays same** | Dataset `jmi_analytics.skill_demand_monthly_latest`; optional month filter; subtitle about non-additive tags. |
| **Must change** | Title to “Skill tag ranking (bar)”; remove donut-specific phrasing in subtitle. |
| **Story change?** | **Presentation only** — still composition/rank of tags. |

---

## S1-TREEMAP-LOC — Location treemap

| Item | Rule |
|------|------|
| **Trigger** | Labels illegible; too many micro-tiles; “Other” invisible. |
| **Fallback visual** | **Highlight table** (already have **S1-HIGHLIGHT-LOC**) — **promote it**: full width, **data bars** on `job_count`, **increase row height**. Optionally **hide treemap** entirely for that layout. |
| **Alternate fallback** | **Horizontal bar**, **Top 15 only** from **`location_top15_other` WHERE location_label <> 'Other'`** plus **separate single KPI or text** for **Other** count — use only if table feels “plain” but treemap fails. **Do not** add map (frozen excluded). |
| **Stays same** | View `jmi_analytics.location_top15_other`. |
| **Story change?** | **No** — same buckets and sums. |

---

## S1-HIGHLIGHT-LOC — Location highlight table

| Item | Rule |
|------|------|
| **Trigger** | Looks too plain for a “premium” row; stakeholders want visual mass. |
| **Fallback** | **Treemap** is primary per spec — if table is weak, **strengthen formatting** (banded rows, bold header, data bars) rather than adding a second chart type. |
| **Story change?** | **No**. |

---

## S1-PARETO-ROLE — Combo chart

| Item | Rule |
|------|------|
| **Trigger** | Line not visible; dual axis not applied; 99 bars too dense; performance slow. |
| **Fallback A** | Keep **combo** but **filter `pareto_rank`** to **≤ 40** in a **new Athena view** `role_pareto_head40` — **only if** cumulative line still computed over **full** set: *disallowed* — would break 100% endpoint. **Do not use truncated Pareto for line.** |
| **Fallback B (allowed)** | **Two visuals side by side:** (1) **Line only** — `pareto_rank` vs `cumulative_job_pct` (shows S-curve clearly); (2) **Bar** — `pareto_rank` vs `job_count` for **`pareto_rank` ≤ 30** with **tooltip `role`**. Full tail in **S1-TABLE-ROLE** only. |
| **Fallback C (allowed)** | **Replace combo** with **line chart only** (`pareto_rank`, `cumulative_job_pct`) + **existing Top 20 table** for magnitudes — reduces clutter; bars dropped. |
| **Stays same** | Dataset `role_pareto` for line; table unchanged. |
| **Story change?** | **Slight:** bar magnitude for rare titles less visible — **mitigate** with Top 20 table. |

---

## S1-TABLE-ROLE — Top 20 table

| Item | Rule |
|------|------|
| **Trigger** | Long strings clip; unreadable on projector. |
| **Fallback** | **Increase column width**, **wrap text**, **reduce font** on other visuals to free space; **abbreviate** title column **not** allowed without new field — prefer **tooltip** on Pareto for full string. |
| **Story change?** | **No**. |

---

## S1-TREEMAP-COMPANY — Company treemap

| Item | Rule |
|------|------|
| **Trigger** | Unreadable; “Other” dominates; long company names overlap. |
| **Fallback visual** | **Highlight table** on same dataset `company_top12_other` — sort `job_count` desc, data bars, **wrap** `company_label`. |
| **Second-line fallback** | **Packed bubbles** (if available in your QS) — **Size:** `job_count`, **Group:** `company_label` — only if table “too plain” and bubbles render cleanly. |
| **Stays same** | View `company_top12_other`. |
| **Story change?** | **No**. |

---

## S1-KPI strip — KPIs

| Item | Rule |
|------|------|
| **Trigger** | `location_hhi` or `company_hhi` shows **null** (no rows in dimension). |
| **Fallback** | Display **“N/A”** via QuickSight **conditional formatting** or **calculated field** `coalesce(cast(location_hhi as string), 'N/A')` — **do not** fabricate zero. |
| **Story change?** | **No** — honest missing data. |

---

## Sheet-level fallback

| Trigger | Action |
|---------|--------|
| Dashboard too tall | Move **S1-HIGHLIGHT-LOC** below treemap only on small screens — **duplicate layout** not allowed; use **Sheet 1** **taller canvas** or **freeform** resize. |
| SPICE stale | Switch dataset to **Direct Query** temporarily for demo, or **Refresh Now**. |

---

## Explicitly disallowed fallbacks

- Replacing failed treemaps with **four horizontal bar charts** for every dimension.  
- Adding **map** for locations (frozen excluded).  
- Adding **pipeline_run_summary** to Sheet 1 for any reason.
