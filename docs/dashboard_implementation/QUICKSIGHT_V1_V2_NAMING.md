# QuickSight & Athena naming: v1 vs v2

Human-readable naming for manual dashboard work. **No architecture change** — this doc is about **labels** and **mental load**.

**Current v2 analysis:** For **minimal attach set**, **approved display names** (`v2_eu_kpis`, `v2_in_locations`, … — **no** raw Glue names in titles), **role_groups vs roles**, and **detach** rules, use **`QUICKSIGHT_V2_DATASET_STRATEGY.md`** (especially **§0** and **§5**). It **supersedes** the older `v2 - EU - …` examples below for active rebuilds.

## A. Recommended naming convention

### Athena / Glue (do **not** rename databases or tables)

The **version boundary is already encoded** in the database names:

| Layer        | Legacy / fallback | Active / current |
|-------------|-------------------|------------------|
| Gold facts  | `jmi_gold`        | `jmi_gold_v2`    |
| Analytics   | `jmi_analytics`   | `jmi_analytics_v2` |

**Keep these names as-is.** Renaming Glue databases or table names would break DDL, `deploy_athena_v2.py`, IAM, data source ARNs, and any dataset SQL. Use **`_v2` suffix** in docs and UIs as the distinguisher.

### QuickSight dataset **display names** (safe to normalize)

Use a **short ASCII prefix** so lists sort together:

| Prefix | Meaning |
|--------|---------|
| `v2 - EU - …` | Active analysis; **Europe / Arbeitnow**; backed by `jmi_analytics_v2` (and optionally `jmi_gold_v2`) |
| `v2 - IN - …` | Active analysis; **India / Adzuna**; backed by `jmi_analytics_v2` / `jmi_gold_v2` |
| `v2 - CMP - …` | Active analysis; **Comparison** helpers (`jmi_analytics_v2` comparison views) |
| `v2 - Gold - …` | Optional: dataset points **directly** at a `jmi_gold_v2.*` **table** (no analytics view) |
| `v1 - legacy - …` | Fallback / old path; typically `jmi_analytics` or `jmi_gold` **without** relying on v2 deploy |

**Pattern:** `v2 - <EU|IN|CMP|Gold> - <short_object_name>`

Examples:

- `v2 - EU - sheet1_kpis`
- `v2 - IN - role_group_pareto_adzuna`
- `v2 - CMP - comparison_benchmark_aligned_month`
- `v2 - Gold - skill_demand_monthly` (if you add a raw-gold dataset)

**Legacy checklist names** (`DS_SHEET1_KPIS`, `DS_SKILLS`, …) map to **`v1 - legacy - <same token>`**, e.g. `v1 - legacy - DS_SHEET1_KPIS`, **only** if that dataset still points at **`jmi_analytics`** / **`jmi_gold`**.

---

## B. Safe to rename **now** (QuickSight only)

Renaming a dataset in the **QuickSight console** updates the **display name** only; **DataSetId** and **ARN** stay the same, so **existing analyses and dashboards keep working**.

**Safe:**

- Any QuickSight **dataset** display name → add `v1 - legacy -` or `v2 - EU -` / `v2 - IN -` / `v2 - CMP -` per the table below.
- **Analysis** or **dashboard** titles (optional) → e.g. append `(v2)` for the active rebuild.

**Prefer renaming in-place** over duplicate-and-delete to avoid orphan datasets and broken references.

---

## C. Should **not** rename **now**

| Object | Reason |
|--------|--------|
| Glue / Athena **database** names (`jmi_gold`, `jmi_gold_v2`, …) | Breaks DDL, scripts, and all SQL |
| Glue **table** / **view** names inside those DBs | Breaks views, deploy script, QuickSight physical table bindings |
| **S3** prefixes | Data layout contract |
| QuickSight **Data source** ARNs / IDs | Unless you are migrating accounts; not needed for naming clarity |
| **Published** dashboard you must not touch per runbook | Duplicate analysis first, then rename datasets on the **copy** |

---

## D. Final naming map (v1 vs v2)

### Athena (reference only — **no renames**)

| Concept | v1 (legacy) | v2 (active) |
|---------|-------------|-------------|
| Gold DB | `jmi_gold` | `jmi_gold_v2` |
| Analytics DB | `jmi_analytics` | `jmi_analytics_v2` |
| Same logical view name (e.g. `sheet1_kpis`) | Lives under `jmi_analytics` | Lives under `jmi_analytics_v2` |

### QuickSight dataset display names — **target**

**v2 (attach to current rebuild)** — replace older labels like `JMI v2 EU — …` with:

| Target display name | Glue / Athena object | DB |
|---------------------|----------------------|-----|
| `v2 - EU - sheet1_kpis` | `sheet1_kpis` | `jmi_analytics_v2` |
| `v2 - EU - skill_demand_monthly_latest` | `skill_demand_monthly_latest` | `jmi_analytics_v2` |
| `v2 - EU - location_top15_other` | `location_top15_other` | `jmi_analytics_v2` |
| `v2 - EU - role_pareto` | `role_pareto` | `jmi_analytics_v2` |
| `v2 - EU - role_top20` | `role_top20` | `jmi_analytics_v2` |
| `v2 - EU - company_top12_other` | `company_top12_other` | `jmi_analytics_v2` |
| `v2 - EU - pipeline_run_summary_latest` | `pipeline_run_summary_latest` | `jmi_analytics_v2` |
| `v2 - IN - skill_demand_monthly_adzuna_latest` | `skill_demand_monthly_adzuna_latest` | `jmi_analytics_v2` |
| `v2 - IN - pipeline_run_summary_adzuna_latest` | `pipeline_run_summary_adzuna_latest` | `jmi_analytics_v2` |
| `v2 - IN - location_top15_other_adzuna` | `location_top15_other_adzuna` | `jmi_analytics_v2` |
| `v2 - IN - role_group_pareto_adzuna` | `role_group_pareto_adzuna` | `jmi_analytics_v2` |
| `v2 - IN - role_group_top20_adzuna` | `role_group_top20_adzuna` | `jmi_analytics_v2` |
| `v2 - IN - company_top15_other_clean_adzuna` | `company_top15_other_clean_adzuna` | `jmi_analytics_v2` |
| `v2 - IN - role_group_demand_monthly_adzuna` | `role_group_demand_monthly_adzuna` | `jmi_analytics_v2` |
| `v2 - IN - role_title_classified_adzuna` | `role_title_classified_adzuna` | `jmi_analytics_v2` |
| `v2 - CMP - comparison_source_month_totals` | `comparison_source_month_totals` | `jmi_analytics_v2` |
| `v2 - CMP - comparison_source_skill_mix_aligned_top20` | `comparison_source_skill_mix_aligned_top20` | `jmi_analytics_v2` |
| `v2 - CMP - comparison_benchmark_aligned_month` | `comparison_benchmark_aligned_month` | `jmi_analytics_v2` |

**Optional raw Gold** (if you create datasets on tables):

| Target display name | Table | DB |
|---------------------|-------|-----|
| `v2 - Gold - skill_demand_monthly` | `skill_demand_monthly` | `jmi_gold_v2` |
| `v2 - Gold - location_demand_monthly` | `location_demand_monthly` | `jmi_gold_v2` |
| (etc.) | … | `jmi_gold_v2` |

**v1 (fallback — keep in account, clearly labeled)**

| Target display name | Typical backing | Notes |
|---------------------|-----------------|--------|
| `v1 - legacy - DS_SHEET1_KPIS` | `jmi_analytics.sheet1_kpis` | Or rename to match your **actual** legacy dataset; token after `legacy -` is free-form |
| `v1 - legacy - DS_SKILLS` | `jmi_analytics.skill_demand_monthly_latest` | |
| … | … | Mirror **`QUICKSIGHT_BUILD_CHECKLIST.md`** `DS_*` list with `v1 - legacy -` prefix |

---

## E. Repo scripts

`scripts/quicksight_create_datasets_v2.py` and `scripts/quicksight_create_comparison_datasets_v2.py` use the **`v2 - …`** display names so new creates match this doc.

**If** your account still has datasets named `JMI v2 EU — …`, **rename them in QuickSight** to the `v2 - EU - …` form above before re-running create scripts, or you may create **duplicate** datasets (the EU script does not skip existing names).

---

## F. Cleanup plan (current analysis)

1. In QuickSight **Manage data** → rename datasets to the **Target display name** column (§D).
2. Open the **current v2 analysis** — visuals should be unchanged (same dataset IDs).
3. Optionally rename the **analysis** to include **v2** in the title.
4. Leave **v1** datasets in the account with **`v1 - legacy -`** names; do **not** delete them for naming alone.

---

## G. What stays unchanged for safety

- All **Athena/Glue** identifiers.
- **Pipeline** and **deploy** scripts that reference `jmi_gold_v2` / `jmi_analytics_v2`.
- **Fallback** datasets: rename only **display**; keep **data source** pointed at legacy DB until you intentionally migrate SQL.
