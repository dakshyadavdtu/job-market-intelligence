# Archive: non-v2 Athena / Glue databases (`jmi_gold`, `jmi_silver`, `jmi_analytics`)

This folder preserves metadata needed to recreate the **legacy (non-v2)** databases after their Glue catalog entries were removed. **S3 data is unchanged**; only catalog metadata was dropped.

## v2 databases (not archived here — remain live)

- `jmi_gold_v2`
- `jmi_silver_v2`
- `jmi_analytics_v2`

## Inventory (from Glue at archive time)

### `jmi_gold` — external tables (6)

| Table | Type |
|-------|------|
| `company_hiring_monthly` | EXTERNAL_TABLE |
| `latest_run_metadata` | EXTERNAL_TABLE |
| `location_demand_monthly` | EXTERNAL_TABLE |
| `pipeline_run_summary` | EXTERNAL_TABLE |
| `role_demand_monthly` | EXTERNAL_TABLE |
| `skill_demand_monthly` | EXTERNAL_TABLE |

### `jmi_silver` — tables (0)

The database existed; **`jmi_silver.jobs` had already been dropped** before this archive. No tables remained.

### `jmi_analytics` — views (28)

All **VIRTUAL_VIEW** (Presto/Athena views):

`company_top12_other`, `company_top15_other_clean`, `company_top15_other_clean_adzuna`, `comparison_region_month_totals`, `comparison_region_skill_mix`, `europe_company_hiring_latest_grain`, `india_city_scatter_metrics`, `india_location_month_heatmap`, `india_skill_job_count_boxplot_grain`, `latest_pipeline_run`, `latest_pipeline_run_adzuna`, `location_top15_other`, `location_top15_other_adzuna`, `pipeline_run_summary_adzuna_latest`, `pipeline_run_summary_latest`, `role_group_demand_monthly`, `role_group_demand_monthly_adzuna`, `role_group_pareto`, `role_group_pareto_adzuna`, `role_group_top20`, `role_group_top20_adzuna`, `role_pareto`, `role_title_classified`, `role_title_classified_adzuna`, `role_top20`, `sheet1_kpis`, `skill_demand_monthly_adzuna_latest`, `skill_demand_monthly_latest`.

## `glue_snapshots/` — authoritative exports

JSON from **AWS Glue API** (same catalog Athena uses):

- `jmi_gold_database.json`, `jmi_gold_tables.json`
- `jmi_silver_database.json`, `jmi_silver_tables.json`
- `jmi_analytics_database.json`, `jmi_analytics_tables.json`

Use these to restore **exact** `Parameters` (e.g. partition projection `projection.run_id.values` on Gold fact tables). Repo DDL may not match every live property.

**Views:** Glue stores `ViewOriginalText` (Presto view with embedded base64). For human-readable SQL, prefer `repo_ddl_copies/ATHENA_VIEWS*.sql` or decode the `presto_view` payload in the Glue snapshot.

## `repo_ddl_copies/` — editable SQL from the repo

- Gold external tables: `ddl_gold_*.sql`, `ddl_gold_latest_run_metadata_arbeitnow.sql`, `ddl_gold_latest_run_metadata_adzuna.sql`
- Silver (deprecated flat layout): `ddl_silver_jobs.sql`, `drop_legacy_jmi_silver_flat_table.sql`
- Analytics views: `ATHENA_VIEWS.sql`, `ATHENA_VIEWS_QS_MULTILAYER.sql`

Recreate order: create databases → `jmi_gold` tables → `jmi_silver` (if ever needed) → `jmi_analytics` views (after Gold exists).

## Notes

- `ddl_silver_jobs.sql` defines deprecated `jmi_silver.jobs` with flat `LOCATION` under `silver/jobs/`; do not reuse if you want only the modular `source=` layout.
- `latest_run_metadata_adzuna` DDL exists in repo but **was not present** in live `jmi_gold` at archive time (only 6 tables).
