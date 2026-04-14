# Multi-source storage layout (Bronze → Silver → Gold → Derived)

See **`docs/MIGRATION_V1_V2.md`** for v1/v2 parallel deployment, rollback, and QuickSight strategy.

## Problem this fixes

- **Bronze:** Code already wrote `bronze/source=<slug>/…`, but live S3 may only show Arbeitnow until Adzuna ingest is synced.
- **Silver:** Arbeitnow used a **legacy flat** path (`silver/jobs/ingest_date=…/run_id=…`) while Adzuna used `silver/jobs/source=adzuna_in/…`. That asymmetry hid the modular story.
- **Gold:** All sources wrote under `gold/<table>/ingest_month=…/run_id=…` with only a **`source` column** in Parquet — paths were not source-partitioned, so the lake looked like one mixed bucket.

## Canonical layout (repo + forward-looking S3)

| Layer | Pattern |
|-------|---------|
| **Bronze** | `{data_root}/bronze/source=<slug>/ingest_date=YYYY-MM-DD/run_id=<id>/raw.jsonl.gz` |
| **Silver jobs** | `{data_root}/silver/jobs/source=<slug>/ingest_date=…/run_id=…/part-00001.parquet` |
| **Silver merged** | `{data_root}/silver/jobs/source=<slug>/merged/latest.parquet` |
| **Gold facts** | `{data_root}/gold/<table>/source=<slug>/ingest_month=YYYY-MM/run_id=<id>/part-00001.parquet` |
| **Latest run pointer (EU)** | `{data_root}/gold/source=arbeitnow/latest_run_metadata/part-00001.parquet` |
| **Latest run pointer (Adzuna)** | `{data_root}/gold/source=adzuna_in/latest_run_metadata/part-00001.parquet` |
| **Derived / comparison** | `{data_root}/derived/comparison/` — benchmark Parquet, exports, **not** mixed into source-native Gold |

Slugs: `arbeitnow`, `adzuna_in` (see `AppConfig.source_name` / connectors).

## Athena / Glue

- Gold **external tables** use **partition projection** on **`source`**, **`ingest_month`**, **`run_id`** (see `infra/aws/athena/ddl_gold_*.sql`).
- Parquet **body** columns no longer duplicate **`source`**; `source` is a **partition key** read from the path.
- Views in `docs/dashboard_implementation/ATHENA_VIEWS*.sql` filter `source = 'arbeitnow'` or `'adzuna_in'` as required.

## Migration (live S3 + Athena) — **do not skip**

1. **Deploy** updated Glue DDL (or `CREATE OR REPLACE` / drop+create in a maintenance window). Update **`projection.run_id.values`** and **`projection.source.values`** in Glue to match reality.
2. **Move** existing Gold objects:
   - From: `gold/<table>/ingest_month=…/run_id=…/`
   - To: `gold/<table>/source=arbeitnow/ingest_month=…/run_id=…/`
   - Use `aws s3 sync` or batch copy; **verify** with a single-table dry run first.
3. **Move** `gold/latest_run_metadata/part-00001.parquet` → `gold/source=arbeitnow/latest_run_metadata/part-00001.parquet` and point **`jmi_gold.latest_run_metadata`** / **`jmi_gold_v2.latest_run_metadata_arbeitnow`** LOCATION at the new prefix (see `ddl_gold_latest_run_metadata_arbeitnow.sql` for v2).
4. **Create** `jmi_gold.latest_run_metadata_adzuna` (see `ddl_gold_latest_run_metadata_adzuna.sql`) and run Adzuna Gold so `part-00001.parquet` exists under `gold/source=adzuna_in/latest_run_metadata/`.
5. **Re-run** Athena view scripts (`ATHENA_VIEWS.sql`, `ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql`, `ATHENA_VIEWS_ADZUNA.sql`, `ATHENA_VIEWS_QS_MULTILAYER.sql`).
6. **Bronze Adzuna on S3:** run `ingest_adzuna` with `JMI_DATA_ROOT=s3://…` (or sync local bronze) so `bronze/source=adzuna_in/…` appears.

## Arbeitnow safety

- Silver transform **still** discovers **legacy** Arbeitnow Silver batches (flat `ingest_date=` under `jobs/`) when rebuilding merged state, until you migrate those objects.
- Streamlit dashboard resolves **`data/gold/<table>/source=arbeitnow/…`** first, then falls back to legacy **`data/gold/<table>/…`** (no `source=` segment).

## Comparison / derived

- Place combined benchmark datasets (e.g. cross-region aggregates produced by a dedicated job) under **`derived/comparison/`**, not under `gold/source=*`.
