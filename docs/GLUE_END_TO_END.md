# AWS Glue in Job Market Intelligence — end to end

This document describes **what AWS Glue does** in this project, from physical data in S3 through to BI tools, and what Glue **does not** do here.

---

## 1. What “Glue” means in AWS

**AWS Glue** is a family of services. In JMI we use **only one part**:

| Glue capability | Used in JMI? |
|-----------------|--------------|
| **Glue Data Catalog** | **Yes** — central **metadata store** for databases, tables, columns, SerDe, S3 locations, and **table properties** (including partition projection). |
| **Glue Crawlers** | **No** (not the primary design). Schema and partitions are defined by **repo DDL** and pipeline layout. |
| **Glue ETL / Spark jobs** | **No**. Transforms run in **Python** locally or on **Lambda** (`src/jmi/pipelines/*`). |
| **Glue Data Quality / Workflows** | **No** in repo automation. |

**Amazon Athena** is the SQL engine. It reads **table and view definitions from the Glue Data Catalog** and reads **bytes from S3** according to those definitions. **QuickSight** (when using Athena) also ultimately relies on that same catalog metadata (column names and types) exposed through Athena.

So in practice: **Glue is the catalog; S3 is the storage; Athena is the query engine.**

---

## 2. End-to-end data path (Glue’s place)

```
APIs / pipelines (Lambda)
        │
        ▼
   Amazon S3  ←── Parquet / JSONL.gz files (Bronze, Silver, Gold)
        │
        ▼
Glue Data Catalog  ←── CREATE EXTERNAL TABLE / CREATE VIEW (DDL in repo)
   (databases, tables, views, TBLPROPERTIES)
        │
        ▼
   Amazon Athena  ←── SELECT … WHERE source / posted_month / run_id …
        │
        ▼
   QuickSight (optional) ←── datasets point at Athena / Glue objects
```

**Glue does not move or transform data** in this stack. It **does not** run Spark. It **stores metadata** so Athena knows:

- **Which S3 prefix** is “table X” (`LOCATION`).
- **Column names and types** for Parquet.
- **How partitions are laid out** (`PARTITIONED BY` and/or **partition projection**).

---

## 3. What gets registered in the Glue Data Catalog

DDL lives mainly under `infra/aws/athena/`. Deploy scripts (e.g. `scripts/deploy_athena_v2.py`) run these statements against Athena; Athena **creates or updates Glue catalog entries**.

### 3.1 Databases (namespaces)

| Database | Role |
|----------|------|
| **`jmi_gold_v2`** | External **tables** over **Gold** Parquet (monthly facts, `latest_run_metadata_*` pointers). |
| **`jmi_silver_v2`** | External **tables** over **Silver** merged Parquet (row-level jobs). |
| **`jmi_analytics_v2`** | **Views only** (SQL definitions; no separate S3 table data). |

Older **`jmi_gold`** / **`jmi_analytics`** may exist in some accounts; v2 is the active modular layout.

### 3.2 External tables (physical S3 data)

Examples:

- **`jmi_gold_v2.skill_demand_monthly`** (and role, location, company, pipeline_run_summary):  
  - `CREATE EXTERNAL TABLE … STORED AS PARQUET`  
  - `LOCATION 's3://<bucket>/gold/<table>/'`  
  - `PARTITIONED BY (source, posted_month, run_id)`  
  - **`TBLPROPERTIES`** enable **partition projection** (see §4).

- **`jmi_gold_v2.latest_run_metadata_arbeitnow`** / **`latest_run_metadata_adzuna`**:  
  - Small **single-row** Parquet pointers (`run_id` only).  
  - **No** partition projection; one `LOCATION` per table.

- **`jmi_silver_v2.arbeitnow_jobs_merged`** / **`adzuna_jobs_merged`**:  
  - Merged Silver Parquet at fixed paths (e.g. `…/merged/`).  
  - **No** partition columns in DDL for that snapshot layout (see `ddl_silver_v2_*.sql`).

### 3.3 Views (logical layer; still Glue metadata)

**`CREATE OR REPLACE VIEW jmi_analytics_v2.<name> AS …`** registers **view definitions** in the **same Glue Data Catalog**. Views do not own S3 objects; they store **SQL text** that Athena resolves at query time.

Files such as `infra/aws/athena/analytics_v2_*.sql` and `docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql` define these views.

---

## 4. Partition projection (the important Glue behavior)

For Gold fact tables, DDL sets **`TBLPROPERTIES`** so Athena can **plan partition paths without listing every partition in Glue** (no `MSCK REPAIR` for each new folder).

Typical pattern (see `infra/aws/athena/ddl_gold_skill_demand_monthly.sql`):

- **`projection.enabled` = `true`**
- **`partition.source`**: `enum` — e.g. `arbeitnow,adzuna_in`
- **`partition.posted_month`**: `date` with range `2018-01` … `2035-12`
- **`partition.run_id`**: **`enum`** — comma-separated list of **actual** `run_id` directory names that exist under S3

**Why `run_id` is enum:** New Gold runs create new `run_id=` folders under S3. The **list of values** in Glue must match reality or Athena **will not scan** those prefixes. The repo ships example values; **live accounts** must **update** `projection.run_id.values` when new runs appear.

**Operational sync (code):** After Gold Lambda writes Parquet, `src/jmi/aws/athena_projection.py` **lists S3** under `gold/role_demand_monthly/`, collects `run_id` segments, and runs **`ALTER TABLE jmi_gold_v2.<table> SET TBLPROPERTIES ('projection.run_id.values'='…')`** via Athena for each of the five partitioned fact tables. That **updates Glue table properties** so projection stays aligned with S3.

---

## 5. What Glue does **not** do in this project

1. **Glue does not run the pipeline** — Lambda + Python do.
2. **Glue does not ingest from APIs** — connectors write to S3 first.
3. **Glue ETL / Spark** — not used for transforms.
4. **Crawlers** — not used as the source of truth; explicit DDL + projection is preferred (see `docs/cost_guardrails.md`, `README.md`).
5. **Glue does not enforce row-level security** — that is application + IAM + Athena scope.

---

## 6. Downstream consumers of Glue metadata

| Consumer | How it uses Glue |
|----------|------------------|
| **Athena** | Resolves table/view names, columns, types, `LOCATION`, and projection rules to **plan** and **read** S3. |
| **QuickSight** | Uses Athena datasets; **column types** for SPICE / direct query come from the catalog **via** Athena. |
| **IAM / Lake Formation** (if used) | Can restrict catalog access; not detailed in this repo’s DDL. |

---

## 7. Failure modes tied to Glue (troubleshooting)

| Symptom | Often related to |
|---------|------------------|
| Athena returns **0 rows** though S3 has files | Stale **`projection.run_id.values`**, wrong **`LOCATION`**, or legacy **`storage.location.template`** on a table — see `docs/aws_live_fix_gold_projection.md`. |
| New `run_id` in S3 not visible | **Projection** not updated; run **`sync_gold_run_id_projection_from_s3`** or extend enum manually in Glue. |
| View errors | View SQL in Glue out of date vs repo; redeploy view DDL. |

---

## 8. Repo map (Glue-related artifacts)

| Path | Purpose |
|------|---------|
| `infra/aws/athena/ddl_gold_*.sql` | Gold external tables + **TBLPROPERTIES** for projection |
| `infra/aws/athena/ddl_silver_v2_*.sql` | Silver merged tables |
| `infra/aws/athena/analytics_v2_*.sql` | Analytics **views** in `jmi_analytics_v2` |
| `docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql` | Comparison **views** |
| `scripts/deploy_athena_v2.py` | Runs DDL to create/update **Glue** objects |
| `src/jmi/aws/athena_projection.py` | **ALTER TABLE** to refresh **`projection.run_id.values`** on Glue |

---

## 9. One-sentence summary

**Glue holds the catalog metadata (databases, external tables, views, partition projection) that tells Athena how to read S3; it does not run ETL or crawlers in this project; keeping `projection.run_id.values` in sync with S3 is the main operational Glue-related task after each Gold run.**
