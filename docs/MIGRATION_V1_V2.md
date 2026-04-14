# v1 → v2 migration: parallel, reversible, QuickSight-safe

This document defines **v1** (current live Arbeitnow-first state) vs **v2** (modular multi-source layout + new BI), with **rollback** at every stage. **No destructive in-place migration** is required to start v2.

---

## 1. v1 / v2 strategy

| | **v1** | **v2** |
|---|--------|--------|
| **Role** | Production dashboard + known-good paths | Modular sources + rebuilt QuickSight |
| **S3 Gold facts** | `gold/<table>/ingest_month=…/run_id=…/` (legacy; no `source` in path) *or* whatever is live today | `gold/<table>/source=<slug>/ingest_month=…/run_id=…/part-00001.parquet` |
| **Metadata** | Often `gold/latest_run_metadata/` (single pointer) | `gold/source=arbeitnow/latest_run_metadata/`, `gold/source=adzuna_in/latest_run_metadata/` |
| **Glue / Athena** | Existing `jmi_gold` + `jmi_analytics` as deployed | **New** database or **new** tables/views (recommended below) |
| **QuickSight** | Published “v1” dashboard — **do not mutate** until cutover | **New** analysis + **new** dashboard (v2) |
| **Git** | Tag **`v1-freeze`** on commit that still matches live | Branch **`feature/v2-multisource`**; tag **`v2-ready`** when validated |

**Coexistence:** v1 and v2 can live in the **same bucket** as long as **key prefixes differ** (legacy vs `source=` segment) and **Glue** either keeps **legacy table DDL** for v1 and adds **v2 tables** pointing only at v2 paths, or you use a **separate** Glue database for v2 (cleanest rollback).

---

## 2. Exact rollback points

| Stage | Rollback |
|-------|-----------|
| **Repo/code** | `git revert` / checkout `v1-freeze`; redeploy pipeline from v1 tag if needed |
| **S3 v2 copy** | Delete or ignore `gold/<table>/source=…` prefixes; v1 keys untouched |
| **Glue v2** | Drop `jmi_gold_v2` / v2 tables; v1 tables unchanged |
| **Athena v2 views** | `DROP VIEW` / use database `jmi_analytics_v2` only |
| **QuickSight v2** | Delete or archive v2 analysis/dashboard; users stay on v1 |
| **Cutover** | Point users back to v1 dashboard URL; SPICE refresh from v1 datasets |

---

## 3. Repo / code steps first (done or to do in Git)

1. **Canonical paths in code** — `src/jmi/paths.py` **`gold_fact_partition`**: `gold/<table>/source=<slug>/ingest_month=…/run_id=…`.
2. **Streamlit** — `dashboard/app.py` **`_gold_table_root`**: `data/gold/<table>/source=arbeitnow/` preferred; legacy `data/gold/<table>/` fallback.
3. **Transforms** — `transform_gold.py` / `transform_silver.py` already aligned with modular Silver + Gold (verify with local run).
4. **Athena SQL** — `docs/dashboard_implementation/ATHENA_VIEWS*.sql` filter `source = 'arbeitnow'|'adzuna_in'`; deploy against **v2** tables when ready.
5. **Commit discipline** — one commit: `fix(paths): canonical table-first Gold layout for v2`; tag **`v1-freeze`** *before* that commit on main if main still tracks production; or branch from production tag, then merge.

---

## 4. Live AWS order (after repo validation)

1. **Local validation** — full pipeline for **arbeitnow** and **adzuna_in** under `data/`; confirm Parquet paths match **C** below; Streamlit reads Gold.
2. **S3 v2 upload / copy** — copy **new** outputs to **v2 prefixes** (`source=` under each table). **Do not delete** v1 keys.
3. **Glue v2 readiness** — Option **A:** new database **`jmi_gold_v2`** + tables with `LOCATION s3://…/gold/<table>/` (same bucket) and projection including **`source`**. Option **B:** new tables in same DB with suffix `_v2`. Point **only** v2 Athena workgroup/tests at these.
4. **View recreation** — create **`jmi_analytics_v2`** (or `CREATE VIEW` names with `_v2` suffix) from repo SQL, pointed at **`jmi_gold_v2`**.
5. **QuickSight v2** — new data source if needed; **new** datasets → **new** analysis → **new** dashboard; **do not** edit v1 published asset.
6. **Cutover** — when row counts + spot queries match policy, share **v2** dashboard; keep v1 read-only for fallback.
7. **Decommission v1** — later: stop writes to legacy paths; archive; **not** on day one.

---

## 5. QuickSight v2 rebuild strategy

- **New** datasets (Athena → `jmi_analytics_v2` or `*_v2` views).
- **New** analysis named e.g. `JMI v2 Multi-source`.
- **New** dashboard; optional **folder** “JMI v2”.
- **SPICE:** schedule refresh after Gold runs; **Direct Query** for development.
- **Permissions:** clone v1 entitlement groups or add v2 resource ARNs; **do not** remove v1 until cutover.

---

## 6. Cutover criteria

- [ ] Athena: `SELECT COUNT(*)` on v2 Gold tables **> 0** for both `source=arbeitnow` and `source=adzuna_in` (when Adzuna is in scope).
- [ ] `latest_run_metadata_arbeitnow` / `latest_run_metadata_adzuna` (Glue `jmi_gold_v2`) and pointer Parquet files exist; views resolve.
- [ ] **jmi_analytics_v2** (or equivalent) views run **without** error.
- [ ] QuickSight v2: **all** visuals show expected non-null data for a test run.
- [ ] Stakeholder sign-off **or** parallel run period (v1 + v2 both available).

---

## 7. Emergency fallback

1. **Stop** SPICE refresh on v2 datasets (optional).
2. **Communicate** “official” dashboard = v1 URL.
3. **No** S3 delete of v1 paths; pipeline can stay on v1 branch/tag until restored.
4. **Git:** `git checkout v1-freeze` for hotfix branch if v2 code must be disabled in Lambda/CI.

---

## Recommended Glue pattern for maximum safety

- **`jmi_gold`** + **`jmi_analytics`** = **v1** (legacy paths / current live) — **frozen** until retirement.
- **`jmi_gold_v2`** + **`jmi_analytics_v2`** = **v2** — all modular DDL from `infra/aws/athena/ddl_gold_*.sql` with **LOCATION** `s3://<bucket>/gold/<table>/` (same bucket allowed; v2 keys only under `source=` folders).

This gives **instant** rollback: QuickSight and Athena queries flip back to v1 database objects without restoring S3.

---

## Canonical path reference (locked)

| Asset | Pattern |
|-------|---------|
| Bronze | `{root}/bronze/source=<slug>/ingest_date=…/run_id=…/raw.jsonl.gz` |
| Silver | `{root}/silver/jobs/source=<slug>/ingest_date=…/run_id=…/part-00001.parquet` |
| Gold facts | `{root}/gold/<table>/source=<slug>/ingest_month=<YYYY-MM>/run_id=<id>/part-00001.parquet` |
| Metadata EU | `{root}/gold/source=arbeitnow/latest_run_metadata/part-00001.parquet` |
| Metadata Adzuna | `{root}/gold/source=adzuna_in/latest_run_metadata/part-00001.parquet` |
| Derived | `{root}/derived/comparison/` |

---

## Git / GitHub discipline

1. **`git tag -a v1-freeze -m "Last commit matching pre-v2 live"`** on the commit that mirrors current production (before v2 merges).
2. Develop v2 on **`feature/v2-multisource`**; open PR; require CI (if any) + local pipeline smoke.
3. **`git tag -a v2-ready -m "Repo validated; ready for AWS v2 deploy"`** after merge to main when local + review complete.
4. **Releases:** GitHub Release notes: “v2 storage layout — see docs/MIGRATION_V1_V2.md”.
