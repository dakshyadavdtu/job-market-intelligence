# Multi-layer QuickSight build (Europe / India / Comparison)

**Locked rules:** Do **not** edit the **published** Arbeitnow dashboard. Duplicate the analysis (or create a new analysis + dashboard) and wire **new datasets** only there.

**Athena:** Run in order: `ATHENA_VIEWS.sql` → `ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql` → `ATHENA_VIEWS_ADZUNA.sql` → **`ATHENA_VIEWS_QS_MULTILAYER.sql`**.

---

## PART 1 — Adaptation decisions (concrete)

### Europe / Arbeitnow — **keep unchanged (logic)**

| Item | Keep |
|------|------|
| `DS_SHEET1_KPIS` → `sheet1_kpis` | Six KPIs |
| `DS_ROLE_GROUP_PARETO` → `role_group_pareto` | Combo / Pareto |
| `DS_ROLE_GROUP_TOP20` → `role_group_top20` | Table |
| `DS_COMPANY_TOP15_CLEAN` → `company_top15_other_clean` | **Main** concentration treemap (employers) |
| `DS_SKILLS` → `skill_demand_monthly_latest` | Composition (donut or stacked bar) |
| `DS_PIPELINE_SUMMARY` | Sheet 2 proof only (unchanged analysis path) |

### Europe — **change slightly (reduce repetition)**

| Current | Action |
|---------|--------|
| `DS_LOC_TOP15` + **treemap** + **highlight table** | **Demote** location treemap if it duplicates “mass inequality” with company treemap: prefer **horizontal bar** or rely on **KPI K3** for top-3 share; **keep** highlight table **or** treemap — **not both** as large hero visuals. |
| (new) | **Gauge** on `sheet1_kpis` — e.g. `location_hhi` or `top3_location_share` (scale in QS). |
| (new) | **Histogram** on `jmi_analytics.europe_company_hiring_latest_grain` — field `job_count` (employer-size distribution). |

### India / Adzuna — **build (new datasets)**

| Dataset (suggested name) | Athena view |
|---------------------------|-------------|
| `DS_IN_MAP` / use existing loc | `location_top15_other_adzuna` or raw `india_location_month_heatmap` for heat |
| `DS_IN_HEAT` | `india_location_month_heatmap` |
| `DS_IN_SCATTER` | `india_city_scatter_metrics` |
| `DS_IN_BOX` | `india_skill_job_count_boxplot_grain` |
| `DS_IN_SKILLS` | `skill_demand_monthly_adzuna_latest` |
| `DS_IN_ROLE_*` | `role_group_pareto_adzuna`, `role_title_classified_adzuna`, etc. as needed |
| `DS_IN_PIPELINE` | `pipeline_run_summary_adzuna_latest` |
| `DS_IN_COMPANY` | `company_top15_other_clean_adzuna` — **optional**; avoid if it clones Europe treemap story |

**Visuals to build:** Filled map (anchor), heat map, box plot, scatter/bubble; optional Sankey/Radar/point map only after honest helper views exist.

### Comparison — **build (new datasets)**

| Dataset | Athena view |
|---------|----------------|
| `DS_CMP_MONTH` | `comparison_region_month_totals` |
| `DS_CMP_SKILL_MIX` | `comparison_region_skill_mix` |

**Visuals:** Line or area on `DS_CMP_MONTH`; 100% stacked column on `DS_CMP_SKILL_MIX`; clustered bar for side-by-side totals; waterfall/radar only if you add dedicated views later.

---

## New `jmi_analytics` views (from `ATHENA_VIEWS_QS_MULTILAYER.sql`)

| View | Purpose |
|------|---------|
| `europe_company_hiring_latest_grain` | Histogram / employer grain (latest EU `run_id`) |
| `india_location_month_heatmap` | Heat map: `location_label` × `ingest_month` |
| `india_city_scatter_metrics` | Scatter: `city_job_count` vs `city_share_of_national` |
| `india_skill_job_count_boxplot_grain` | Box plot grain (skills) |
| `comparison_region_month_totals` | Trend: EU vs IN total postings by month |
| `comparison_region_skill_mix` | Mix: skill-tag shares within region |

---

## Implementation order

1. **Athena:** Confirm multilayer SQL applied (see repo file).
2. **QuickSight:** Create datasets (Athena → `jmi_analytics`) for **new** views + existing Adzuna views.
3. **Duplicate** analysis from template or create **new** analysis — **do not** open published dashboard.
4. **Sheets:** Europe (classic) | India (geo/matrix/distribution) | Comparison (benchmark).
5. **QA:** Refresh SPICE; validate totals vs `QA_VALIDATION_CHECKLIST.md` where applicable.

---

## Automation vs manual

| Step | Automation | Manual |
|------|------------|--------|
| Athena `CREATE VIEW` | CLI / script | — |
| QuickSight datasets | `create-data-set` API possible | Console often faster |
| Visual layout, gauge min/max, map geocoding | — | Console |
| Publish **new** dashboard | API possible | Console |

---

## Empty-data note

If `europe_company_hiring_latest_grain` returns **0** rows, `latest_run_metadata_arbeitnow` may point at a `run_id` with no `company_hiring_monthly` partitions yet, or Glue projection enum may need the latest `run_id`. Fix **data/projection**, not the view definition.
