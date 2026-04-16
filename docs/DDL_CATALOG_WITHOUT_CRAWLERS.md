# JMI: catalog via DDL (no Glue crawlers)

This project **does not** rely on **AWS Glue Crawlers** (or ad-hoc **`MSCK REPAIR TABLE`**) as the primary way to register S3 data. Instead, **metadata is defined explicitly** with **Athena DDL** checked into the repo, executed via **`aws athena start-query-execution`** (or the same calls from Python scripts). The **pipeline** owns the **S3 key layout** and **column types**; the catalog **mirrors** that contract.

**Why (project stance):** See `docs/cost_guardrails.md` — *“Avoid crawlers unless absolutely needed”* and *“Use manual DDL + controlled partition adds in MVP”*. Crawlers add **S3 LIST** cost, **non-deterministic** schema drift, and conflict with **partition projection** enums that this repo maintains for **`run_id`**.

---

## 1. What a crawler would do (and what we do instead)

| Crawler behavior | JMI approach |
|------------------|--------------|
| Scan S3 prefixes to **infer** columns and partitions | **`CREATE EXTERNAL TABLE`** with **fixed** column lists in `infra/aws/athena/ddl_*.sql` matching **`src/jmi/pipelines`** outputs |
| Register **discovered** partitions in Glue | **Partition projection** (`TBLPROPERTIES` in DDL) so Athena **constructs** paths from predicates + enums — no per-run partition registration |
| Periodic runs to “catch up” | **Redeploy DDL** when schema changes; **extend** `projection.run_id.values` after Gold runs (see §4) |
| Schema surprises when vendor JSON changes | **Bronze** keeps raw JSON; **Silver/Gold** schema is **code-defined** — you change Python + DDL together in PRs |

---

## 2. Where DDL lives (this repo)

| Area | Location |
|------|----------|
| **Gold v2** external tables + projection | `infra/aws/athena/ddl_gold_*.sql` (facts + `latest_run_metadata_*`) |
| **Silver v2** merged tables | `infra/aws/athena/ddl_silver_v2_arbeitnow_merged.sql`, `ddl_silver_v2_adzuna_merged.sql` |
| **Analytics views** | `infra/aws/athena/analytics_v2_*.sql`, `docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql`, etc. |
| **Legacy / archaeology** | `infra/aws/athena/archive_non_v2_ddl/` (not for live deploy) |

**Glue Data Catalog** still stores the result: Athena **`CREATE TABLE` / `CREATE VIEW`** creates or updates **Glue** table entries. There is **no separate** “non-Glue” catalog — we simply **avoid Crawler-generated** tables.

---

## 3. How DDL is applied (canonical v2 deploy)

**Entry point:** `scripts/deploy_athena_v2.py`

1. **`CREATE DATABASE IF NOT EXISTS jmi_gold_v2`** (and analytics DB as needed).
2. Runs **patched** Gold DDL files (`patch_ddl`: `jmi_gold` → `jmi_gold_v2`, optional refresh of **`projection.run_id.values`** from a repo-maintained list).
3. Executes each **`CREATE EXTERNAL TABLE`** statement via **Athena** (workgroup + `ResultConfiguration` → `s3://<bucket>/athena-results/`).
4. Chains **Python deployers** for analytics: `deploy_jmi_analytics_v2_minimal.py`, `deploy_athena_comparison_views_v2.py`, `deploy_comparison_v2_views.py`, `drop_presentation_layer_athena.py`.
5. Drops **obsolete** views by name so the catalog matches the **minimum** current set.

**Prerequisites:** AWS CLI credentials, **same region** as the bucket, **Athena workgroup** (default `primary`), and an **S3 output prefix** for query results (see `src/jmi/aws/athena_projection.py` `athena_output_uri()` pattern).

**Silver:** Deploy Silver DDL with the same pattern when you add or change merged tables (see `scripts/` that reference `ddl_silver_v2_*.sql` if present in your workflow).

---

## 4. Partitions without `MSCK REPAIR` or crawlers

Gold fact tables use **`TBLPROPERTIES`** for **partition projection** (example: `infra/aws/athena/ddl_gold_skill_demand_monthly.sql`):

- **`projection.enabled` = true**
- **`projection.source.values`** — `arbeitnow,adzuna_in`
- **`projection.posted_month`** — date range
- **`projection.run_id.values`** — **comma-separated enum** of pipeline `run_id` segments that **exist under S3**

New Gold runs add **`run_id=`** folders. **Crawlers are not used** to discover them. Instead:

- **`src/jmi/aws/athena_projection.py`** — **`sync_gold_run_id_projection_from_s3()`** lists `gold/role_demand_monthly/`, collects `run_id` values, runs **`ALTER TABLE jmi_gold_v2.<table> SET TBLPROPERTIES ('projection.run_id.values'='…')`** for each fact table (Gold Lambda after write).
- **`scripts/deploy_athena_v2.py`** — **`update_gold_v2_run_id_projection`** can patch the same property from a **CSV list** when deploying.

This keeps Athena **consistent** with **explicit** S3 layout — the same layout **`transform_gold.py`** writes.

---

## 5. Views as DDL (not physical tables)

**`jmi_analytics_v2`** is almost entirely **`CREATE OR REPLACE VIEW`**. Views are **versioned SQL** in Git; redeploying updates **Glue** view definitions **without** rewriting Parquet.

Scripts such as **`deploy_athena_comparison_views_v2.py`** split SQL files and run **`CREATE OR REPLACE VIEW`** per statement. No crawler is involved.

---

## 6. When you change the pipeline

1. **Change S3 paths or Parquet schema** in `src/jmi/` first.  
2. **Update matching DDL** (`LOCATION`, columns, partition keys).  
3. **Run** `deploy_athena_v2.py` (or targeted SQL) in a maintenance window.  
4. **Extend** `projection.run_id.values` (automatic via Lambda sync or manual) — **no crawler run**.

---

## 7. Related docs in this repo

| Doc | Topic |
|-----|--------|
| `docs/GLUE_END_TO_END.md` | Glue catalog vs ETL; projection |
| `docs/aws_live_fix_gold_projection.md` | Fixing stale projection / bad `TBLPROPERTIES` |
| `docs/cost_guardrails.md` | Crawler avoidance, DDL preference |
| `README.md` §14 | Athena + Glue workflow |

---

## 8. One-line summary

**JMI treats the catalog as infrastructure-as-code:** **DDL files + scripts** register **exact** S3 locations and **partition projection**; **Glue Crawlers are not** the source of truth for Bronze/Silver/Gold in this project.
