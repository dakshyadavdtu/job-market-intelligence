# `jmi_analytics` inventory and cleanup policy (audit)

**Audited via:** AWS Glue `get-tables` (metadata only; no scans). Do **not** drop objects here without re-checking repo SQL and QuickSight dependencies.

## `jmi_gold` (6 tables)

All are **required**: base Gold external tables + **`jmi_gold_v2.latest_run_metadata_arbeitnow`** (EU) / **`latest_run_metadata_adzuna`** as applicable. **No** analytics-layer removal.

| Name | Role |
|------|------|
| `skill_demand_monthly`, `role_demand_monthly`, `location_demand_monthly`, `company_hiring_monthly`, `pipeline_run_summary` | Partition-projected facts |
| `latest_run_metadata_arbeitnow` | Single-row `run_id` for latest EU (Arbeitnow) run views (`jmi_gold_v2`) |

## `jmi_analytics` (core + Adzuna + multilayer helpers)

**Core Arbeitnow (production):** `sheet1_kpis`, `skill_demand_monthly_latest`, `location_top15_other`, `role_group_pareto`, `role_group_top20`, `company_top15_other_clean`, `pipeline_run_summary_latest`, `latest_pipeline_run`, `role_title_classified`, `role_group_demand_monthly`, `role_pareto`, `role_top20`, `company_top12_other` — see `QUICKSIGHT_BUILD_CHECKLIST.md` §A3.

**Adzuna slice (`ATHENA_VIEWS_ADZUNA.sql`):** `latest_pipeline_run_adzuna`, `skill_demand_monthly_adzuna_latest`, `pipeline_run_summary_adzuna_latest`, `location_top15_other_adzuna`, `role_title_classified_adzuna`, `role_group_*_adzuna`, `company_top15_other_clean_adzuna`, etc.

**QuickSight multilayer helpers (`ATHENA_VIEWS_QS_MULTILAYER.sql`):** `europe_company_hiring_latest_grain`, `india_location_month_heatmap`, `india_city_scatter_metrics`, `india_skill_job_count_boxplot_grain`, `comparison_region_month_totals`, `comparison_region_skill_mix` — see `QUICKSIGHT_MULTILAYER_BUILD.md`.

| View | Class | Notes |
|------|--------|------|
| `sheet1_kpis` | **Production** | Locked dataset `DS_SHEET1_KPIS` |
| `skill_demand_monthly_latest` | **Production** | `DS_SKILLS` |
| `location_top15_other` | **Production** | `DS_LOC_TOP15` |
| `role_group_pareto` | **Production** | `DS_ROLE_GROUP_PARETO` |
| `role_group_top20` | **Production** | `DS_ROLE_GROUP_TOP20` |
| `company_top15_other_clean` | **Production** | `DS_COMPANY_TOP15_CLEAN` |
| `pipeline_run_summary_latest` | **Production** | `DS_PIPELINE_SUMMARY` |
| `latest_pipeline_run` | **Helper** | Used by almost all latest-run views; **do not drop** |
| `role_title_classified` | **Helper** | Feeds `role_group_demand_monthly` → `role_group_*`; **do not drop** |
| `role_group_demand_monthly` | **Helper** | Intermediate for pareto/top20; **do not drop** |
| `role_pareto`, `role_top20` | **Optional drill-down** | Raw titles; keep for optional visuals / QA |
| `company_top12_other` | **Legacy / QA** | Superseded by `company_top15_other_clean` for final demo; **still** referenced by `ATHENA_VIEWS.sql`, `QA_VALIDATION_CHECKLIST.md`, `VISUAL_FALLBACK_RULES.md` — **do not drop** from Athena unless those docs are updated and fallbacks revised |

**Cleanup decision:** **No** `DROP VIEW` in Athena for this pass. Every view is either production, a dependency of production, optional drill-down, or legacy-but-documented.

## Glue maintenance (unchanged)

After each Gold run, append the new **`run_id`** to **`projection.run_id.values`** on all five partitioned `jmi_gold` tables. See `docs/aws_live_fix_gold_projection.md`.

## QuickSight (account hygiene)

- **API access** works with the `jmi` profile for listing datasets/analyses/dashboards.
- **Duplicate datasets** (same name, different IDs) and **per-table exploratory analyses** may exist from early experiments. **Do not** bulk-delete datasets via API without checking **every** analysis and the published dashboard.
- Published dashboard **`DEA dashboard1 dakshharsh`** references **multiple** dataset ARNs (mix of raw `jmi_gold`-backed and `jmi_analytics` views). For a **polished final demo**, edit the dashboard in the **QuickSight console** to use **only** the **seven locked production datasets** (`QUICKSIGHT_BUILD_CHECKLIST.md` §A3), then remove **orphan** datasets that have **zero** references.
- **Full visual build** from the CLI (`create-dashboard` with `Definition`) is possible but **not** maintained in this repo; follow **`QUICKSIGHT_BUILD_CHECKLIST.md`** sections B–E in the UI.
