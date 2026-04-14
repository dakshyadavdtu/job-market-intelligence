# Live fix: Gold partition projection (Glue metadata)

Use this when **S3 Gold has data** but **Athena base tables or `jmi_analytics` latest-run views return no rows**, often after older Glue tables were created with a custom `storage.location.template`.

**Repo source of truth:** `infra/aws/athena/ddl_gold_*.sql`, `docs/dashboard_implementation/ATHENA_VIEWS.sql`, `src/jmi/pipelines/transform_gold.py`.

---

## Why this happens

1. **Stale Glue catalog:** Tables were created earlier with `storage.location.template` in table properties. Current repo DDL **does not** set that property; Athena is meant to use **default Hive-style** paths under each table `LOCATION` (`ingest_month=<yyyy-MM>/run_id=<id>/`), matching the Gold writer.
2. **`CREATE TABLE IF NOT EXISTS` does not update** existing tables. Re-running repo DDL in Athena **without** dropping or editing Glue leaves **old** `TBLPROPERTIES` in place.
3. **Partition projection** still applies: with projection enabled, Athena **ignores** registered partitions in Glue for that table and uses **only** the projection config + query predicates. Wrong or obsolete `storage.location.template` can make projected paths **not** line up with real S3 prefixes.

**Not affected:** `jmi_gold.latest_run_metadata` / `jmi_gold_v2.latest_run_metadata_arbeitnow` (not partitioned, single `LOCATION`). The pointer can show the right `run_id` while partitioned fact tables still scan wrong paths.

---

## `run_id`: use `enum`, not `injected` (for latest-run views)

With **`projection.run_id.type` = `injected`**, Athena engine 3 can return **`CONSTRAINT_VIOLATION`** for `jmi_analytics.*` views that filter `run_id` via **`INNER JOIN latest_pipeline_run`** (or any non-literal predicate). **Literal** `WHERE run_id = '…'` works; **JOIN**-based “latest run” does not.

**Durable fix:** set **`projection.run_id.type` = `enum`** and **`projection.run_id.values`** = comma-separated list of **every** `run_id` directory that exists under the table `LOCATION` (append each new Gold `run_id` after each run via **Glue** `update-table` or console). Repo DDL uses **`enum`** with example values—extend the list in Glue when new runs land.

**Ongoing:** after each successful Gold run, append the new `run_id` to **`projection.run_id.values`** on **all five** partitioned Gold tables (same list on each). Automate with a small Glue/API step in the pipeline if you want zero manual edits.

---

## Why fix in Glue first (preferred)

- **Same** database and table names → **`jmi_analytics` views** keep working without `DROP`/`CREATE` of views.
- **QuickSight** datasets that point at `jmi_analytics.*` stay valid (no retargeting).
- **No** pipeline or S3 layout change.

**Drop/recreate** Gold tables is a **fallback** only if you cannot remove the bad property or other metadata is irreconcilable.

---

## Affected tables (exact names)

In database **`jmi_gold`**, these **five** partitioned external tables must **not** have `storage.location.template` if you follow current repo DDL:

| Table |
|-------|
| `skill_demand_monthly` |
| `role_demand_monthly` |
| `location_demand_monthly` |
| `company_hiring_monthly` |
| `pipeline_run_summary` |

**Not in this list:** `latest_run_metadata` (no partition projection for this fix path).

---

## Property to remove

- **Name:** `storage.location.template`  
- **Where:** AWS Glue → **Tables** → each affected table → **Table properties** / **Parameters** (same key Athena shows in `TBLPROPERTIES`).

**Action:** **Delete** this property entirely for each of the five tables, then **Save**.

**Optional alignment:** Confirm the remaining projection keys match the repo (e.g. `projection.enabled`, `projection.ingest_month.*`, `projection.run_id.type` = `enum` with up-to-date `projection.run_id.values`). Edit in Glue if an old table drifted.

**`LOCATION`:** Must point at your **actual** Gold prefix for that dataset (same bucket/prefix the pipeline writes). If the repo DDL uses a placeholder bucket, your live table may already override `LOCATION`—do **not** change bucket names casually; only fix if you know live data lives elsewhere.

---

## Validate in Athena

After saving Glue changes, wait a few seconds, then run **`infra/aws/athena/validate_gold_projection_fix.sql`** in the Athena query editor (same **Region** and **workgroup** as usual).

**Success looks like:**

- `latest_pipeline_run` returns one `run_id`.
- **Base Gold** `COUNT(*)` queries with **both** `run_id` (from subquery) **and** `ingest_month BETWEEN '2018-01' AND '2035-12'` return **> 0** when that run has rows for that grain (skills can be 0 if no skills in silver—then check role/location/pipeline summary).
- **`jmi_analytics.skill_demand_monthly_latest`** and **`pipeline_run_summary_latest`** return rows when upstream data exists.
- **`sheet1_kpis`** returns at least one row when KPI inputs exist for the latest run.

---

## QuickSight (minimal)

- **SPICE** datasets: **Refresh** after Athena validates.
- **Direct Query:** Usually no change; reload the analysis if needed.

---

## What not to do

- Do **not** rely on **`CREATE TABLE IF NOT EXISTS`** alone to clear `storage.location.template` on existing tables.
- Do **not** run **`MSCK REPAIR`** as the primary fix for these projected Gold tables; normal operation is **partition projection** + predicates (see `QUICKSIGHT_BUILD_CHECKLIST.md`).
- Do **not** **drop** the five Gold tables unless in-place Glue correction failed and you accept recreating tables and temporarily breaking dependent views until DDL + `ATHENA_VIEWS.sql` are re-applied.
- Do **not** change S3 layout or Gold transform logic for this fix—the issue is **catalog metadata**, not Parquet layout.

---

## Fallback: only if Glue in-place fix fails

1. Drop **`jmi_analytics`** views that reference `jmi_gold` (or all views from `ATHENA_VIEWS.sql` order—your environment may require dropping dependents first).
2. **`DROP TABLE`** each of the five partitioned `jmi_gold.*` tables.
3. Re-run **`infra/aws/athena/ddl_gold_*.sql`** `CREATE` statements from the repo (adjust `LOCATION` bucket in SQL **only** if your live bucket differs and you maintain a forked DDL).
4. Re-run **`docs/dashboard_implementation/ATHENA_VIEWS.sql`** (and optional quality views file).

---

## Related files

| File | Purpose |
|------|---------|
| `infra/aws/athena/validate_gold_projection_fix.sql` | Post-fix Athena checks |
| `infra/aws/athena/ddl_gold_*.sql` | Target Glue/Athena table definitions |
| `docs/dashboard_implementation/ATHENA_VIEWS.sql` | `jmi_analytics` views |
