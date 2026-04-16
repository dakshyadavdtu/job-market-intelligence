# S3 bucket layout — folders, roles, and pipeline flow

This document describes **top-level prefixes** under the JMI data bucket (the URI in **`JMI_DATA_ROOT`**, typically `s3://<bucket>/`). It reflects **`src/jmi/paths.py`**, **`src/jmi/config.py`**, and the ingest → Silver → Gold chain.

**Scope:** Canonical and operational prefixes used by this repo. **`archive/`** (if you maintain such a prefix manually) is **out of scope** here. **`infra/aws/athena/archive_non_v2_ddl/`** is a **Git** archive of old DDL, not an S3 folder.

---

## End-to-end flow (where folders sit)

```
APIs / connectors
       │
       ▼
  bronze/          ← raw immutable snapshots (JSONL.gz + manifest)
       │
       ▼
  silver/          ← deduped job rows (Parquet) + merged/latest
  silver_legacy/   ← optional legacy Silver batches (read-only union for Gold/Silver repair)
       │
       ▼
  gold/            ← monthly aggregates + latest_run_metadata pointers
  gold_legacy/     ← optional old Gold layout (not written by current pipeline)
       │
       ├── quality/     ← JSON QA artifacts from transforms
       ├── health/      ← small “latest ingest” JSON for ops
       └── state/       ← incremental connector state (watermarks)

  athena-results/  ← Athena query output (not pipeline data)
  lambda_legacy/     ← optional Lambda .zip audit artifact
  derived/           ← optional sync target for ad-hoc exports (see scripts)
```

---

## 1. `bronze/`

| | |
|---|---|
| **Essence** | **Immutable raw layer**: vendor JSON **as landed**, plus pipeline stamps (`run_id`, `bronze_ingest_date`, lineage). |
| **Role in flow** | **Ingest** (`ingest_live`, `ingest_adzuna`, …) writes here first. Silver **reads** the gzip JSONL for a single batch path. Nothing in Silver “fixes” Bronze files—they stay for **audit and replay**. |

**Canonical pattern** (all sources):

```text
bronze/source=<slug>/ingest_date=YYYY-MM-DD/run_id=<pipeline-run-id>/raw.jsonl.gz
bronze/.../manifest.json
```

- **`<slug>`** examples: `arbeitnow`, `adzuna_in`.  
- **Arbeitnow slice experiments:** `source=arbeitnow/slice=<tag>/ingest_date=…/run_id=…/` when `JMI_ARBEITNOW_SLICE` is set.

**Files:**

- **`raw.jsonl.gz`** — one JSON object per line (Bronze record with `raw_payload`).  
- **`manifest.json`** — batch metadata: counts, paths, incremental diagnostics, watermarks.

---

## 2. `silver/`

| | |
|---|---|
| **Essence** | **Clean, typed job table** (Parquet) with a **fixed column contract** (`silver_schema.py`). |
| **Role in flow** | **Silver transform** reads **one** Bronze `raw.jsonl.gz`, writes a **per-batch** Parquet and updates a **merged** snapshot. |

**Canonical modular layout:**

```text
silver/jobs/source=<slug>/ingest_date=YYYY-MM-DD/run_id=<bronze-run-id>/part-00001.parquet
silver/jobs/source=<slug>/merged/latest.parquet
```

**Slice layout (Arbeitnow + `JMI_ARBEITNOW_SLICE`):**

```text
silver/jobs/source=arbeitnow/slice=<tag>/ingest_date=…/run_id=…/part-00001.parquet
silver/jobs/source=arbeitnow/slice=<tag>/merged/latest.parquet
```

- **Per-batch files:** one row per surviving `job_id` after dedupe for that run.  
- **`merged/latest.parquet`:** rolling **deduped** union of history (newer `job_id` wins per merge rules). Gold reads Silver for aggregates via this path or explicit batch paths.

---

## 3. `silver_legacy/`

| | |
|---|---|
| **Essence** | **Historical Silver** batches from the **pre–`source=`** era (flat `ingest_date=` / `run_id=` under `jobs/`). |
| **Role in flow** | **Not written** by the current modular writer. Silver/Gold **may still read** these paths when merging or unioning Arbeitnow history (`load_silver_jobs_history_union` in `transform_silver.py`). Use this to **avoid mixing** old flat keys with the current `silver/jobs/source=…` tree. |

---

## 4. `gold/`

| | |
|---|---|
| **Essence** | **Analytics-ready aggregates**: small Parquet facts by **skill, role, location, company**, plus **pipeline_run_summary** and **per-source latest run pointers**. |
| **Role in flow** | **Gold transform** reads Silver (merged or union), assigns **`posted_month`** from posting time, aggregates, writes Parquet. **Athena** external tables point at these prefixes. |

**Fact tables** (examples):

```text
gold/<table_name>/source=<slug>/posted_month=YYYY-MM/run_id=<pipeline-run-id>/part-00001.parquet
```

**`<table_name>`** includes: `skill_demand_monthly`, `role_demand_monthly`, `location_demand_monthly`, `company_hiring_monthly`, `pipeline_run_summary`.

**Latest run pointer (one row Parquet per source):**

```text
gold/source=arbeitnow/latest_run_metadata/part-00001.parquet
gold/source=adzuna_in/latest_run_metadata/part-00001.parquet
```

**Arbeitnow slice isolation** (`JMI_ARBEITNOW_SLICE`):

```text
gold/slice=<tag>/<table>/<source>/<posted_month>/…/part-00001.parquet
gold/slice=<tag>/source=<slug>/latest_run_metadata/part-00001.parquet
```

- **Partition grain:** **`posted_month`** = calendar month of the **job posting** (from Silver `posted_at`), **not** the pipeline clock date.  
- **`run_id`** in the path = **Gold pipeline run id** (`pipeline_run_id` in code).

---

## 5. `gold_legacy/`

| | |
|---|---|
| **Essence** | **Old Gold** objects that used **`ingest_month=`** (or other retired layouts) under `paths.py` comments—**not** produced by **`gold_fact_partition`** today. |
| **Role in flow** | **Read-only** if you still have historical files; **migrators** may move old keys here. Current writers use **`gold/`** + **`posted_month=`** + **`source=`**. |

---

## 6. `quality/`

| | |
|---|---|
| **Essence** | **JSON quality reports** for each transform stage. |
| **Role in flow** | **Silver** writes `silver_quality_<bronze_ingest_date>_<run_id>.json`. **Gold** writes `gold_quality_<pipeline_run_id>.json`. Used for **debugging, audits, and proof** that checks passed or counts matched. |

---

## 7. `health/`

| | |
|---|---|
| **Essence** | **Small operational snapshots** of the **latest successful ingest** (source, run id, counts, paths to Bronze/manifest). |
| **Role in flow** | **Ingest** (`ingest_live`) writes **`latest_ingest.json`** (or slice-specific name). **Dashboards / ops** can point here without scanning all of Bronze. |

---

## 8. `state/`

| | |
|---|---|
| **Essence** | **Incremental connector state** (e.g. fetch watermarks for the next Arbeitnow pull). |
| **Role in flow** | **Ingest** persists state after a successful batch (`bronze_incremental.py`). Ensures **micro-batch** runs are **idempotent** and bounded. |

---

## 9. `athena-results/`

| | |
|---|---|
| **Essence** | **Athena query output** (CSV or similar) for **ad-hoc SQL and DDL** (`ALTER TABLE`, `CREATE VIEW`). |
| **Role in flow** | **Not** part of the medallion datasets. **Glue/Athena** and deploy scripts use this as **`ResultConfiguration.OutputLocation`**. Required for any **StartQueryExecution** API call. |

---

## 10. `lambda_legacy/`

| | |
|---|---|
| **Essence** | **Optional** stored **Lambda deployment zip** (`jmi-lambda.zip`) for **audit or download**. |
| **Role in flow** | **Production Lambdas** use **ECR images**, not this zip. Upload is **optional** (`infra/aws/lambda/README.md`). |

---

## 11. `lambda/` (bucket root)

| | |
|---|---|
| **Essence** | Intended to stay **empty** in the documented design. |
| **Role in flow** | **Not** used for live Lambda code; live code is **ECR**. |

---

## 12. `derived/` (optional)

| | |
|---|---|
| **Essence** | **Optional** prefix for **comparison exports** or benchmarks **not** mixed into source-native Gold (`STORAGE_LAYOUT_MULTISOURCE.md`). |
| **Role in flow** | **`scripts/pipeline_live_sync.py`** can sync a local `derived/` tree to S3 if present. **Not** required for core Bronze → Silver → Gold. |

---

## Summary table

| Prefix | Medallion / ops | Written by pipeline? |
|--------|------------------|----------------------|
| `bronze/` | Raw audit layer | Yes (ingest) |
| `silver/` | Cleaned jobs | Yes (Silver) |
| `silver_legacy/` | Legacy Silver | No (historical) |
| `gold/` | Aggregates + metadata | Yes (Gold) |
| `gold_legacy/` | Legacy Gold | No (historical) |
| `quality/` | QA JSON | Yes |
| `health/` | Latest ingest JSON | Yes (ingest) |
| `state/` | Incremental state | Yes (ingest) |
| `athena-results/` | Query scratch | Athena clients |
| `lambda_legacy/` | Zip artifact | Optional manual upload |
| `lambda/` | Empty | N/A |
| `derived/` | Optional exports | Optional |

---

## Source of truth in code

- **Path builders:** `src/jmi/paths.py`  
- **Roots:** `AppConfig` in `src/jmi/config.py` (`bronze_root`, `silver_root`, `gold_root`, `quality_root`, `health_root`, `state_root`)  
- **Multi-source narrative:** `docs/STORAGE_LAYOUT_MULTISOURCE.md`, `docs/MIGRATION_V1_V2.md`
