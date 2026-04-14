# QuickSight v2 — minimal dataset strategy (current analysis)

**Scope:** Attach/detach for the **active v2 analysis** only. **Do not** delete global datasets. **Do not** rename Athena/Glue objects — only **QuickSight dataset display names** here.

**Principle:** One dataset ≈ one **logical domain** (or one unavoidable SQL view). **Never** name datasets after chart types or raw view names.

---

## 0. Final naming review (approved)

### A. Critical review

| Name | Verdict |
|------|---------|
| **`v2_eu_role_groups`** / **`v2_in_role_groups`** | **Keep.** They mean **classified role families** (normalized buckets from `role_title_classified*`), **not** “groups of visuals.” Pairing with **`v2_eu_roles`** / **`v2_in_roles`** (raw **title** grain from Gold) avoids confusion: **roles** = raw strings, **role_groups** = taxonomy. If “groups” ever reads oddly, synonym in docs only: *role families*. |
| **`v2_eu_companies`** | **Keep.** Backs **cleaned employer** analytics (`company_top15_other_clean*`), not raw posting strings — the name is **domain** (“companies”), not “clean” or “top15.” |
| **`v2_cmp_skills`** | **Keep.** Backs **aligned** cross-source skill mix SQL; display name stays **short** — implementation detail “aligned_top20” stays in Glue only. |
| **`v2_in_kpis`** | **No first-class Athena view** in repo for India sheet-1 KPIs (no `sheet1_kpis` in `ATHENA_VIEWS_ADZUNA.sql`). **Do not** add **`v2_in_kpis`** as a dataset until a view exists or you **blend** Gold in QS. Until then, India “KPI-like” strips use **`v2_in_skills`**, **`v2_in_pipeline`**, **`v2_in_role_groups`**, etc. |

### B. Raw vs derived (naming split)

| Display name | Domain | Typical backing |
|----------------|--------|-----------------|
| **`v2_*_roles`** | Raw **job-title** buckets | Gold `role_demand_monthly` |
| **`v2_*_role_groups`** | **Classified** families (derived) | Analytics `role_group_pareto*` |
| **`v2_*_locations`** | Raw **location** buckets | Gold `location_demand_monthly` |
| **`v2_*_companies`** | **Derived** employer dimension (cleaned / rolled) | Analytics `company_top15_other_clean*` |
| **`v2_*_skills`** | Skill **tags** (market demand) | Gold `skill_demand_monthly` |
| **`v2_*_kpis`** | **Composite** KPI row (EU only today) | Analytics `sheet1_kpis` |

No separate “raw companies” dataset in the **minimal** set; add later only if you need **`company_hiring_monthly`** alongside **clean**.

### C. `role_group_top20` / `role_group_top20_adzuna` — **remove from attach**

**Yes — replace with `role_group_pareto*` + QuickSight filter.**

Both **pareto** and **top20** views use the **same** aggregation: for the latest `run_id`, sum `job_count` across `posted_month` per `role_group`, then assign **`pareto_rank` = ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY job_count DESC, role_group ASC)`** (see `ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql` ~144–177 and ~183–212; Adzuna ~253–286 and ~292–337). The **top20** view is literally **`WHERE pareto_rank <= 20`** on that ranked set — same ordering as **pareto**.

The **pareto** view adds **`share_of_total`** and **`cumulative_job_pct`** on the **same** rows (all families). For any visual that only needs the top 20 rows: **filter `pareto_rank <= 20`** on **`v2_*_role_groups`**. Row counts and `role_group` / `job_count` / `pareto_rank` match the top20 view for those rows.

**Attach a second dataset** for top20 only if you must **SPICE** a tiny extract for cost — not required for correctness.

### D. Do **not** change (now)

- **Glue / Athena** names: `role_group_pareto`, `company_top15_other_clean`, `comparison_source_skill_mix_aligned_top20`, etc.
- **QuickSight DataSetId** (internal IDs).
- **Published** analysis names if locked by process — optional cosmetic rename only.

---

## 1. Candidate inventory (backing object → role)

All objects live in **`jmi_gold_v2`** (tables) or **`jmi_analytics_v2`** (views) unless noted.

### Europe / Arbeitnow

| Backing object | Role |
|----------------|------|
| `sheet1_kpis` | Month-level KPI block: totals, HHI, top-3 location share, top-1 role share (multi-fact SQL). |
| `role_group_pareto` | Role **families** (classified titles) with rank / share / cumulative % — *requires `ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql` deployed to `jmi_analytics_v2`*. If missing, use `role_pareto` (raw titles) temporarily. |
| `role_group_top20` | Subset of family rows — **redundant** if `role_group_pareto` is attached (filter `pareto_rank <= 20` in QS). |
| `role_pareto` / `role_top20` | Raw **role** strings — optional drill-down if you also use families. |
| `location_top15_other` | Pre-rolled top-N + “Other” for locations — **optional**; duplicates semantics you can approximate from Gold + Top-N in QS. |
| `company_top15_other_clean` | Employer names after legal/suffix normalization + roll-up — **not** reproducible honestly from raw Gold without copying long SQL. |
| `skill_demand_monthly_latest` | Thin filter on latest run — **drop**; use **`skill_demand_monthly`** + `run_id` / `latest_run_metadata_arbeitnow` (EU) or `latest_run_metadata_adzuna`. |
| `pipeline_run_summary_latest` | Thin filter — **drop**; use **`pipeline_run_summary`** + filters. |
| `role_demand_monthly` (Gold) | Raw role buckets: combo, histogram, table, scatter, Pareto **if** you stay on raw titles. |
| `location_demand_monthly` (Gold) | Locations: maps, heat maps, histograms, treemap (with QS Top-N). |
| `skill_demand_monthly` (Gold) | Skills: composition, histogram. |
| `company_hiring_monthly` (Gold) | Raw employers — **usually skip** for EU if `company_top15_other_clean` is the canonical employer story. |
| `pipeline_run_summary` (Gold) | Run proof / row counts. |
| `latest_run_metadata_arbeitnow` (Gold EU) | Single-row `run_id` for parameters (optional tiny dataset). |

### India / Adzuna

| Backing object | Role |
|----------------|------|
| `role_group_pareto_adzuna` | Same as EU but families for India. |
| `role_group_top20_adzuna` | **Redundant** with pareto + filter. |
| `role_title_classified_adzuna` | Audit / raw→family — **optional** unless you build an explicit audit visual. |
| `role_group_demand_monthly_adzuna` | Month × family — **optional** for dense heat/matrix. |
| `company_top15_other_clean_adzuna` | Normalized employers (India). |
| `location_top15_other_adzuna` | Optional pre-bucketed locations — **prefer Gold** + QS for flexibility. |
| `skill_demand_monthly_adzuna_latest` / `pipeline_run_summary_adzuna_latest` | **Drop** — use Gold + filters. |
| Gold fact tables (`source = adzuna_in`) | Same pattern as EU. |

### Comparison

| Backing object | Role |
|----------------|------|
| `comparison_source_month_totals` | Posting volume by `source` × `posted_month` (each source’s latest run). |
| `comparison_source_skill_mix_aligned_top20` | Aligned month, top skills, renormalized shares — **hard** to reproduce in QS. |
| `comparison_benchmark_aligned_month` | One aligned month: role totals + skill-tag HHI — **proof / narrative row**. |
| `comparison_source_skill_mix` | Full skill long tail — **optional** if `aligned_top20` covers all skill-mix visuals. |
| `comparison_source_month_skill_tag_hhi` | HHI time series — **optional** if benchmark row is enough. |

---

## 2. Per-dataset decision

Display names are in §5. “**Gold**” = `jmi_gold_v2.<table>`.

| QS display name (target) | Backing | Verdict | Reason | Gold+filters enough? |
|---------------------------|---------|---------|--------|------------------------|
| **v2_eu_kpis** | `sheet1_kpis` | **Keep** | Multi-table KPI + HHI math belongs in SQL. | **No** — would need multiple blends and fragile calcs. |
| **v2_eu_role_groups** | `role_group_pareto` (or `role_pareto`) | **Keep** | Family classification + cumulative %; **pareto** view also supports a “top N” table via filter. | **No** for families; **yes** for raw titles only (then use Gold only — see below). |
| **v2_eu_roles** | `role_demand_monthly` | **Keep** | Raw title grain: histogram, scatter, optional raw drill. | **Yes** — this **is** Gold. |
| **v2_eu_locations** | `location_demand_monthly` | **Keep** | Maps, heat map, histogram, location treemap (Top-N in QS). | **Yes** — Gold. |
| **v2_eu_companies** | `company_top15_other_clean` | **Keep** | Normalized employer dimension; not honest to rebuild in QS. | **No** for cleaned semantics. |
| **v2_eu_skills** | `skill_demand_monthly` | **Keep** | Skill demand grain. | **Yes** — Gold. |
| **v2_eu_pipeline** | `pipeline_run_summary` | **Keep** (when sheet needs proof) | Run-quality row counts. | **Yes** — Gold; attach when you build that sheet. |
| **v2_eu_run** | `latest_run_metadata_arbeitnow` | **Optional** | One-row `run_id` for default parameters. | **Yes** — or set parameter manually / from first visual. |
| **v2_in_role_groups** | `role_group_pareto_adzuna` | **Keep** | Same as EU families. | **No** for families. |
| **v2_in_roles** | `role_demand_monthly` | **Keep** | Raw roles, `source = adzuna_in`. | **Yes** — Gold. |
| **v2_in_locations** | `location_demand_monthly` | **Keep** | Maps / heat / histogram. | **Yes** — Gold. |
| **v2_in_companies** | `company_top15_other_clean_adzuna` | **Keep** | Clean employer story. | **No** for cleaned semantics. |
| **v2_in_skills** | `skill_demand_monthly` | **Keep** | Skill grain. | **Yes** — Gold. |
| **v2_in_pipeline** | `pipeline_run_summary` | **Keep** (when needed) | Proof rows for India. | **Yes** — Gold. |
| **v2_in_classified** | `role_title_classified_adzuna` | **Optional later** | Only if you add an audit / title-quality visual. | N/A — helper for drill, not strictly required if pareto is enough. |
| **`v2_in_families_month`** (optional) | `role_group_demand_monthly_adzuna` | **Optional later** | Month × **classified** family — heat / matrix. | Easier than re-aggregating in QS; name avoids “role_month” ambiguity. |
| **v2_cmp_monthly** | `comparison_source_month_totals` | **Keep** (comparison sheet) | Cross-source monthly volumes. | Possible with two Gold queries + blend — **helper is one clean dataset**. |
| **v2_cmp_skills** | `comparison_source_skill_mix_aligned_top20` | **Keep** | Aligned month + top-skill renormalization. | **No** — fragile in QS. |
| **v2_cmp_benchmark** | `comparison_benchmark_aligned_month` | **Keep** | Benchmark / proof row. | **No** — join logic is in SQL. |
| *Thin `*_latest` views* | various | **Detach** | Redundant with Gold + `run_id`. | **Yes** — use Gold. |
| *`role_group_top20` / `role_group_top20_adzuna`* | — | **Detach** | Redundant with pareto + rank filter. | N/A |
| *`location_top15_other` (both)* | — | **Detach** (default) | Prefer **`v2_*_locations`** Gold for all location visuals; use QS Top-N / “Other” if needed. | **Yes** with more QS work. |

**Detach from current analysis:** every **v1** dataset (`jmi_gold` / `jmi_analytics` without `_v2`), every **`*_latest`** analytics dataset, **`role_group_top20`**, and (by default) **`location_top15_other`** variants — unless you explicitly choose pre-bucketed locations to save QS time.

---

## 3. Final minimal keep-set — whole v2 analysis

**Attach and rename display names only:**

| # | Display name | Athena object |
|---|----------------|---------------|
| 1 | `v2_eu_kpis` | `jmi_analytics_v2.sheet1_kpis` |
| 2 | `v2_eu_role_groups` | `jmi_analytics_v2.role_group_pareto` (or `role_pareto` if family views not deployed) |
| 3 | `v2_eu_roles` | `jmi_gold_v2.role_demand_monthly` |
| 4 | `v2_eu_locations` | `jmi_gold_v2.location_demand_monthly` |
| 5 | `v2_eu_companies` | `jmi_analytics_v2.company_top15_other_clean` |
| 6 | `v2_eu_skills` | `jmi_gold_v2.skill_demand_monthly` |
| 7 | `v2_eu_pipeline` | `jmi_gold_v2.pipeline_run_summary` |
| 8 | `v2_in_role_groups` | `jmi_analytics_v2.role_group_pareto_adzuna` |
| 9 | `v2_in_roles` | `jmi_gold_v2.role_demand_monthly` (filter `source = adzuna_in`) |
| 10 | `v2_in_locations` | `jmi_gold_v2.location_demand_monthly` |
| 11 | `v2_in_companies` | `jmi_analytics_v2.company_top15_other_clean_adzuna` |
| 12 | `v2_in_skills` | `jmi_gold_v2.skill_demand_monthly` |
| 13 | `v2_in_pipeline` | `jmi_gold_v2.pipeline_run_summary` |
| 14 | `v2_in_kpis` | `jmi_analytics_v2.sheet1_kpis_adzuna` |
| 15 | `v2_cmp_monthly` | `jmi_analytics_v2.comparison_source_month_totals` |
| 16 | `v2_cmp_skills` | `jmi_analytics_v2.comparison_source_skill_mix_aligned_top20` |
| 17 | `v2_cmp_benchmark` | `jmi_analytics_v2.comparison_benchmark_aligned_month` |

**Total: 17** datasets for full EU + IN + comparison coverage (see **§5** for the canonical list).

**Optional later (attach only when building that visual):**

- `v2_eu_run` → `latest_run_metadata_arbeitnow` (EU pointer)
- `v2_in_run` → `latest_run_metadata_adzuna`
- `v2_in_kpis` → *no default view* — add only if you ship an India KPI SQL view or accept a blended QS recipe
- `v2_in_classified` → `role_title_classified_adzuna`
- `v2_in_families_month` → `role_group_demand_monthly_adzuna`
- `v2_cmp_skill_mix_full` → `comparison_source_skill_mix`
- `v2_cmp_skill_hhi` → `comparison_source_month_skill_tag_hhi`

---

## 4. First sheet only — absolute minimum

Assume **Sheet 1 = Europe** (Arbeitnow) first:

| Attach now | Purpose |
|------------|---------|
| `v2_eu_kpis` | KPI row + gauges (HHI, shares) |
| `v2_eu_role_groups` | Family combo / cumulative story |
| `v2_eu_locations` | Treemap / map / histogram |
| `v2_eu_companies` | Employer concentration |
| `v2_eu_skills` | Skill composition / histogram |

**Five datasets.** Add **`v2_eu_roles`** if you need **raw-title** visuals on the same sheet without family bucketing. Add **`v2_eu_pipeline`** if the first sheet includes a **proof** strip.

**Defer until India sheet:** all **`v2_in_*`**. **Defer until comparison sheet:** all **`v2_cmp_*`**.

### E. First Europe sheet — minimal attach (recap)

`v2_eu_kpis`, `v2_eu_role_groups`, `v2_eu_locations`, `v2_eu_companies`, `v2_eu_skills` — **five** datasets.

Optional on the same sheet: **`v2_eu_roles`**, **`v2_eu_pipeline`**.

### F. Full v2 analysis — minimal attach (recap)

All **17** rows in **§5 Approved map** (EU 7 + India 7 + Comparison 3), including **`v2_in_kpis`** → `sheet1_kpis_adzuna`.

---

## 5. Approved QuickSight display-name map (rename only in QS)

Use **exactly** these display names for the **current** v2 analysis. **Do not** embed Glue view names in the title.

| QS display name | Glue / Athena object (unchanged) |
|-----------------|----------------------------------|
| `v2_eu_kpis` | `jmi_analytics_v2.sheet1_kpis` |
| `v2_eu_role_groups` | `jmi_analytics_v2.role_group_pareto` (or `role_pareto` if family views not deployed) |
| `v2_eu_roles` | `jmi_gold_v2.role_demand_monthly` |
| `v2_eu_locations` | `jmi_gold_v2.location_demand_monthly` |
| `v2_eu_companies` | `jmi_analytics_v2.company_top15_other_clean` |
| `v2_eu_skills` | `jmi_gold_v2.skill_demand_monthly` |
| `v2_eu_pipeline` | `jmi_gold_v2.pipeline_run_summary` |
| `v2_in_role_groups` | `jmi_analytics_v2.role_group_pareto_adzuna` |
| `v2_in_roles` | `jmi_gold_v2.role_demand_monthly` |
| `v2_in_locations` | `jmi_gold_v2.location_demand_monthly` |
| `v2_in_companies` | `jmi_analytics_v2.company_top15_other_clean_adzuna` |
| `v2_in_skills` | `jmi_gold_v2.skill_demand_monthly` |
| `v2_in_pipeline` | `jmi_gold_v2.pipeline_run_summary` |
| `v2_in_kpis` | `jmi_analytics_v2.sheet1_kpis_adzuna` |
| `v2_cmp_monthly` | `jmi_analytics_v2.comparison_source_month_totals` |
| `v2_cmp_skills` | `jmi_analytics_v2.comparison_source_skill_mix_aligned_top20` |
| `v2_cmp_benchmark` | `jmi_analytics_v2.comparison_benchmark_aligned_month` |

**Optional (not in the 17 core):**

| QS display name | When to use |
|-----------------|-------------|
| `v2_eu_run` / `v2_in_run` | Optional: `latest_run_metadata_arbeitnow` / `latest_run_metadata_adzuna` for parameters. |
| `v2_in_classified` | `role_title_classified_adzuna` — audit / drill. |
| `v2_in_families_month` | `role_group_demand_monthly_adzuna` — month × family matrix (clearer than `role_month` if you add it). |

**Filters in analysis:** `source = 'arbeitnow'` vs `'adzuna_in'` on Gold datasets as required.

**Top-20 family table:** use **`v2_eu_role_groups`** / **`v2_in_role_groups`** with filter **`pareto_rank <= 20`** — **do not** attach `role_group_top20*`.

---

## 6. Detach from current analysis now

- All datasets pointing at **`jmi_gold`** or **`jmi_analytics`** (non-`_v2`).
- All **`*_latest`** analytics datasets (replaced by Gold + `run_id`).
- **`role_group_top20`** / **`role_group_top20_adzuna`** (use **`v2_*_role_groups`** + filter).
- **`location_top15_other`** / **`location_top15_other_adzuna`** if you adopt **`v2_*_locations`** Gold-only policy.
- Duplicate exploratory datasets (same physical table, old names).
- **`company_top12_other`** if still attached.

---

## 7. Visual family → dataset (sanity check)

| Pattern | Use |
|---------|-----|
| KPI row / gauge | `v2_eu_kpis`, **`v2_in_kpis`** (`sheet1_kpis_adzuna`) |
| Treemap | `v2_eu_locations`, `v2_eu_companies`, `v2_eu_skills` (or role_groups) |
| Combo / cumulative line | `v2_eu_role_groups`, `v2_in_role_groups` |
| Histogram | `v2_eu_roles`, `v2_eu_skills`, `v2_eu_locations` |
| Table | Any of the above (filter / sort in QS) |
| Map | `v2_in_locations` / `v2_eu_locations` (string geocoding limits apply) |
| Heat map | `v2_in_locations` or `v2_in_families_month` (optional) |
| Box plot | Measures from `v2_*_roles` / `v2_*_skills` with bins / categories in QS |
| Scatter / bubble | `v2_eu_roles` + `v2_eu_skills` (or same-table measures) |
| 100% stacked (comparison) | `v2_cmp_skills` |
| Line / area (comparison) | `v2_cmp_monthly` |
| Benchmark / proof | `v2_cmp_benchmark`, `v2_eu_pipeline`, `v2_in_pipeline` |
| Radar / waterfall / sankey / funnel | Only add **extra** datasets if you implement those visuals and cannot derive from **`v2_cmp_skills`** / **`v2_cmp_monthly`**; no extra datasets required up front. |

---

## 8. Relation to `QUICKSIGHT_V1_V2_NAMING.md`

The older doc used **`v2 - EU - <view_name>`**. This strategy **supersedes** display names for the **current analysis**: use **`v2_eu_*` / `v2_in_*` / `v2_cmp_*`** — **domain-first**, no raw view names in the title.

---

## 9. Repo scripts

`scripts/quicksight_create_datasets_v2.py` still lists **Glue view names** for **create-data-set** API; **after** creation, **rename** datasets in QuickSight to **`v2_eu_*`** per this doc. Future automation could accept a **display-name map** file — not required for manual rebuild.

---

## 10. FINAL SPEC — frozen v2 QuickSight strategy

**Status:** **Stable** (rev. **India KPI helper**). **v1** datasets must **not** drive the current analysis. **Athena/Glue** object names stay fixed; only QS **display names** and **attachment** matter.

### India KPI — implementation (Phase 1 product gap **closed**)

- **Decision:** India KPIs are backed by a **first-class Athena view** `jmi_analytics_v2.sheet1_kpis_adzuna` (see `ATHENA_VIEWS_ADZUNA.sql`), **not** QuickSight-only hacks on multiple datasets.
- **QuickSight display name:** **`v2_in_kpis`** → `sheet1_kpis_adzuna`.
- **Columns (truthful):** `posted_month`, `run_id`, `active_posted_months`, `total_postings`, `located_postings`, `top3_location_share`, `top1_location_share`, `location_hhi`, `company_hhi`, `top1_role_share`, `distinct_location_buckets`, `distinct_role_title_buckets`, `distinct_role_groups` (from `role_group_demand_monthly_adzuna`).
- **Explicitly omitted:** **remote / hybrid share** — `remote_type` exists only in **Silver**, not in Gold aggregates; adding it would require a **pipeline** change (new Gold fact or view over Silver). **Do not** fake it in QS.

### Advanced visuals — honest verdict

| Idea | Verdict | Notes |
|------|---------|--------|
| **India radar** (skills / families) | **A** — from **`v2_in_skills`** / **`v2_in_role_groups`** | Same caveats as any radar: pick measures that share a scale. |
| **India sankey** | **C — skip** | No honest **source→sink flow** in aggregate market data (no application funnel). |
| **India funnel** | **C — skip** | No defensible **stage** dimension in Gold. |
| **Point / layer map** | **A** — **`v2_in_locations`** | Geocode quality is **data + QS**, not fixed by a helper view. |
| **India heat (month × geo or month × family)** | **A** — `v2_in_locations` or optional **`v2_in_families_month`** | Optional helper only for **month × role family** density. |
| **Comparison waterfall** | **A** — **`v2_cmp_monthly`** | Month-over-month or source deltas via QS table calc / running diff. |
| **Dual radar (EU vs IN)** | **A** — **`v2_cmp_skills`** | One row per source × skill in aligned month. |
| **HHI time series** | **B** — attach optional **`v2_cmp_skill_hhi`** → existing `comparison_source_month_skill_tag_hhi` | Already in **`ATHENA_VIEWS_COMPARISON_V2.sql`**; deploy with `deploy_athena_comparison_views_v2.py`. |
| **Comparison sankey** | **C — skip** | No truthful inter-source **flow** without invented links. |

### PHASE 1 — Final confirmation

1. **Strategy is stable** — **17** datasets in §5 are the full **must-attach** set for EU + IN + comparison (includes **`v2_in_kpis`**).
2. **Still detach** per **§6** (v1, `*_latest` thin views, `role_group_top20*`, optional `location_top15_other*`, `company_top12_other`, duplicates).
3. **Leave unattached for now:** `v2_eu_run`, `v2_in_run`, `v2_in_classified`, `v2_in_families_month`, `v2_cmp_skill_mix_full`, `v2_cmp_skill_hhi` — attach when the specific visual needs them.

### PHASE 2 — Topic-wise lists (A / B / C)

#### Europe / Arbeitnow

| Class | Datasets |
|-------|----------|
| **A. Must keep** | `v2_eu_kpis`, `v2_eu_role_groups`, `v2_eu_roles`, `v2_eu_locations`, `v2_eu_companies`, `v2_eu_skills`, `v2_eu_pipeline` |
| **B. Optional** | `v2_eu_run` → `latest_run_metadata_arbeitnow` (parameter default for `run_id`) |
| **C. Do not attach** | v1-backed datasets; `skill_demand_monthly_latest`; `pipeline_run_summary_latest`; `role_group_top20`; `location_top15_other`; raw `company_top12_other` as primary; duplicate Gold-backed datasets with old names |

#### India / Adzuna

| Class | Datasets |
|-------|----------|
| **A. Must keep** | `v2_in_kpis`, `v2_in_role_groups`, `v2_in_roles`, `v2_in_locations`, `v2_in_companies`, `v2_in_skills`, `v2_in_pipeline` |
| **B. Optional** | `v2_in_run`; `v2_in_classified` (audit); `v2_in_families_month` (month × classified family — heat matrix) |
| **C. Do not attach** | v1; `*_latest` thin views; `role_group_top20_adzuna`; `location_top15_other_adzuna` (if using `v2_in_locations` only) |

#### Comparison

| Class | Datasets |
|-------|----------|
| **A. Must keep** | `v2_cmp_monthly`, `v2_cmp_skills`, `v2_cmp_benchmark` |
| **B. Optional** | `v2_cmp_skill_mix_full` → `comparison_source_skill_mix` (full skill tail); `v2_cmp_skill_hhi` → `comparison_source_month_skill_tag_hhi` (HHI over time vs benchmark-only) |
| **C. Do not attach** | v1 comparison datasets; legacy `comparison_region_*` unless you still maintain those views |

### PHASE 3 — Graph coverage vs final keep-set

| Region / sheet | Visual family | Covered by final keep-set? | Extra helper needed? |
|----------------|---------------|----------------------------|----------------------|
| **Europe** | KPI row, gauge (HHI, shares) | **Yes** — `v2_eu_kpis` | No |
| **Europe** | Treemap | **Yes** — `v2_eu_locations`, `v2_eu_companies`, `v2_eu_skills`, `v2_eu_role_groups` | No |
| **Europe** | Combo / Pareto (families) | **Yes** — `v2_eu_role_groups` (`pareto_rank`, `cumulative_job_pct`); top-20 table → filter `pareto_rank <= 20` | No |
| **Europe** | Histogram | **Yes** — `v2_eu_roles`, `v2_eu_skills`, `v2_eu_locations` | No |
| **Europe** | Table | **Yes** — any of the above | No |
| **Europe** | Scatter / box | **Yes** — `v2_eu_roles` / `v2_eu_skills` + calculated fields / bins | No |
| **India** | KPI row / gauge | **Yes** — **`v2_in_kpis`** (`sheet1_kpis_adzuna`) after deploy | Deploy view first |
| **India** | Filled / point map | **Yes** — `v2_in_locations` (string geocoding limits apply) | No |
| **India** | Heat map (geo or month×loc) | **Yes** — `v2_in_locations`; dense **month × family** → optional **`v2_in_families_month`** | Optional **only** for that matrix |
| **India** | Box, scatter, bubble, histogram | **Yes** — `v2_in_roles`, `v2_in_skills`, `v2_in_locations` | No |
| **India** | Radar | **Yes** — `v2_in_skills` / `v2_in_role_groups` | Sankey/funnel **not recommended** (no honest flow) |
| **Comparison** | Line / area | **Yes** — `v2_cmp_monthly` | No |
| **Comparison** | 100% stacked column | **Yes** — `v2_cmp_skills` | No |
| **Comparison** | Benchmark / proof table | **Yes** — `v2_cmp_benchmark` | No |
| **Comparison** | Waterfall / dual radar | **Yes** — `v2_cmp_monthly` / `v2_cmp_skills` | Optional **`v2_cmp_skill_hhi`** for HHI **time series** |

**Conclusion:** **17** core datasets cover the planned families. Optional: **`v2_in_families_month`**, **`v2_cmp_skill_mix_full`**, **`v2_cmp_skill_hhi`**. **Skip** sankey/funnel unless you redefine the product (not recommended from current data).

### PHASE 4 — Cleanup actions (checklist)

#### E. First Europe sheet only — attach

`v2_eu_kpis`, `v2_eu_role_groups`, `v2_eu_locations`, `v2_eu_companies`, `v2_eu_skills`

**Optional same sheet:** `v2_eu_roles`, `v2_eu_pipeline`

#### F. Full v2 analysis — attach (**17**)

All names in **§5** (`v2_eu_*` ×7, `v2_in_*` ×7 including **`v2_in_kpis`**, `v2_cmp_*` ×3).

#### G. Detach from current analysis — now

- Any dataset on **`jmi_gold`** / **`jmi_analytics`** (not **`_v2`**).
- **`skill_demand_monthly_*_latest`**, **`pipeline_run_summary_*_latest`** (analytics thin views).
- **`role_group_top20`**, **`role_group_top20_adzuna`**.
- **`location_top15_other`**, **`location_top15_other_adzuna`** (if standardizing on Gold **`v2_*_locations`**).
- **`company_top12_other`** (legacy vs clean employers).
- Any duplicate or old-name dataset pointing at the same object as a **`v2_*`** row in §5.

#### H. Gaps before manual QuickSight build

1. **Deploy Athena views:** run **`python scripts/deploy_athena_v2.py`** so **`sheet1_kpis_adzuna`** exists in **`jmi_analytics_v2`** (appended to `ATHENA_VIEWS_ADZUNA.sql`). Run **`deploy_athena_comparison_views_v2.py`** if **`v2_cmp_skill_hhi`** is needed.
2. **EU role families:** `role_group_pareto` + `company_top15_other_clean` require **`ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql`** deployed to **`jmi_analytics_v2`**. If not deployed, temporarily point **`v2_eu_role_groups`** at **`role_pareto`** and **`v2_eu_companies`** at raw Gold or deploy the SQL first.
3. **`run_id` / `source` filters** on Gold datasets — set in analysis or via **`v2_eu_run`** / **`v2_in_run`** optional datasets.
4. **Geospatial:** India maps depend on **location string** quality and QuickSight geocoding — not a missing dataset.
5. **Remote/hybrid KPI:** **not** in `sheet1_kpis_adzuna` — requires **future** Gold aggregate from Silver `remote_type` if product needs it.

---

### Quick reference — output A–H

| ID | Content |
|----|---------|
| **A** | **Confirmed:** strategy stable; detach per §6/G; optional per §10.1–10.3 |
| **B** | **Europe:** §10.2 table — 7 must, 1 optional, C = do not attach |
| **C** | **India:** §10.2 — 6 must, optional block, no `v2_in_kpis` without view |
| **D** | **Comparison:** §10.2 — 3 must, 2 optional |
| **E** | **First sheet:** §10.4.E |
| **F** | **Full analysis:** §10.4.F (**17** datasets, §5) |
| **G** | **Detach:** §10.4.G |
| **H** | **Gaps:** §10.4.H |

---

## 11. Deliverable summary (A–G)

### A. India KPI solution

**Athena helper** `jmi_analytics_v2.sheet1_kpis_adzuna` — QuickSight **`v2_in_kpis`**. Not a QS-only blend.

### B. New helper views (this change)

| Glue view | File |
|-----------|------|
| `sheet1_kpis_adzuna` | `docs/dashboard_implementation/ATHENA_VIEWS_ADZUNA.sql` (end of file) |

**No other new views** — comparison HHI time series already exists as `comparison_source_month_skill_tag_hhi` (optional **`v2_cmp_skill_hhi`**).

### C. Advanced visuals (recap)

| | |
|--|--|
| **Supported now** | Radar (IN/CMP from skill/role mix datasets), maps, heat, waterfall, dual radar, line/area, stacked, benchmark |
| **Optional one helper** | **`v2_cmp_skill_hhi`** for HHI time series |
| **Skip** | Sankey, funnel (no honest stages/flows) |

### D–E. Dataset lists — see **§10.2** and **§5** (**17** must-attach).

### F. Graph coverage matrix — see **§10** “Advanced visuals” + **§7**.

### G. Explicitly skip for now

- **Remote/hybrid share** India KPI (needs Silver→Gold pipeline work).
- **Sankey / funnel** as “market truth” visuals.
- **Extra** `india_flow` / `india_stage` helpers — **not** justified.
