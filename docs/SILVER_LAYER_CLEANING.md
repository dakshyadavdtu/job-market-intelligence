# Silver layer: cleaning and handling of dirty data

This document describes **exactly** how the **Silver** transform (`src/jmi/pipelines/transform_silver.py`) cleans Bronze rows and how **invalid or noisy** inputs are handled. Implementation references point to the current codebase.

---

## 1. What Silver is

- **Input:** Bronze **`raw.jsonl.gz`** — one JSON object per line, preserving **`raw_payload`** plus pipeline fields (`job_id`, `source`, `run_id`, etc.).
- **Output:** **Parquet** with a **fixed column contract** (`CANONICAL_SILVER_COLUMN_ORDER` in `src/jmi/pipelines/silver_schema.py`): one **deduplicated** row per logical job for the batch, plus a **merged** snapshot across history.
- **Principle:** Silver is **typed, normalized, and deduped** for analytics; it does **not** silently accept duplicate keys or empty mandatory dimensions if checks fail.

---

## 2. Ingest hygiene before cleaning

| Step | Behavior | Code |
|------|------------|------|
| **Single source per file** | Every row must match the first row’s `source`; mixed sources **abort** with `RuntimeError`. | `transform_silver.run` |
| **Lineage from path** | `bronze_ingest_date` and `bronze_run_id` are taken from the Bronze **key path** (`ingest_date=…`, `run_id=…`); mismatch with path **raises**. | `_extract_lineage_from_bronze_path` |
| **Empty Bronze** | Empty file → **`RuntimeError`** (no Silver written). | `transform_silver.run` |

So “dirty” structure at the file level (mixed sources) is **rejected**, not cleaned.

---

## 3. Field-level cleaning (Bronze → flat row)

For each Bronze row, the pipeline reads **`raw_payload`** and derives analytic fields.

### 3.1 Text and HTML

| Field | Cleaning | Purpose |
|-------|-----------|---------|
| **Description** | `strip_html_description`: HTML-unescape, remove `<script>` / `<style>`, strip tags, collapse whitespace. | Skill extraction and Adzuna remote inference use **plain text**, not markup. |
| **Generic text** | `_clean_text` / `str(…).strip()` on titles, slugs, etc. | Avoid whitespace-only “values”. |

### 3.2 Title (`title_norm`)

- **Arbeitnow:** `normalize_title_norm` — trim, collapse whitespace, remove common **DE gender parentheticals** `(m/w/d)`-style, trim edge punctuation, **lowercase** for analytics.  
- **Adzuna:** `adzuna_title_norm_for_silver` — same base normalization; if the title is **very short** (≤2 words or fragment like “Head Of”), **append a non-generic category tag** (e.g. `… - it-jobs`) so vague vendor titles gain usable signal.

### 3.3 Company (`company_norm`)

- `normalize_company_norm`: trim, replace `|` with space, collapse whitespace, **lowercase**, optional drop of leading **“The ”** for long strings, strip edge punctuation.

### 3.4 Location (`location_raw`)

- `normalize_location_raw`: lowercasing, comma normalization, **dedupe adjacent duplicate segments**, trim segment punctuation, small **EU alias** (e.g. Frankfurt).  
- **India-specific:** if segments look like India (state/city/country lists), **`_canonicalize_india_location_parts`** unifies shapes (e.g. city-only → `city, state`, state-only → `state, india`). Aliases include **bengaluru → bangalore**, **gurugram → gurgaon**, **orissa → odisha**.  
- **Adzuna object locations:** `adzuna_location_for_silver` prefers **`location.area`** when **`display_name`** is only a country but a richer hierarchy exists (so “India” + area list still yields city/state when possible).

### 3.5 Remote work (`remote_type`)

- **Arbeitnow:** `remote_type_from_arbeitnow_payload` — `remote: true/false` → `remote` / `onsite`; else **`unknown`**.  
- **Adzuna:** no boolean; `remote_type_from_adzuna_payload` scans **title + stripped description + category + contract** for regex patterns (**hybrid**, **remote/WFH**, **onsite/office**). No match → **`unknown`** (explicit, not guessed as onsite).

### 3.6 Posted time (`posted_at`)

- `posted_at_iso_from_payload`: **Arbeitnow** uses Unix or ISO `created_at`; **Adzuna** uses ISO `created`; parses to **UTC ISO-8601** strings. Parse failures → **`None`** (nullable in Silver).

### 3.7 Skills (`skills`)

- **Rule-based only** (no ML): `extract_silver_skills` in `src/jmi/connectors/skill_extract.py` uses an **allowlist**, aliases, stoplists, and phrase/token matching over **tags + title + description**.  
- **Adzuna:** `adzuna_enrich_weak_skills` can add skills when signals are weak, using the same extracted context.  
- Stored in Parquet as a **JSON array string** via `_skills_to_json_str` (empty → `"[]"`).

### 3.8 Identifiers

- **`job_id` / `job_id_strategy`:** Carried from Bronze (computed at ingest).  
- **`source_job_id`:** Adzuna uses vendor `source_job_id` when present; Arbeitnow uses **slug** when present.

---

## 4. Deduplication (within batch and across history)

| Scope | Rule | Rationale |
|-------|------|-----------|
| **Current Bronze batch** | `drop_duplicates(subset=["job_id"], keep="first")` | One row per `job_id` per batch. |
| **Merged snapshot** | Concatenate prior Silver + batch, sort by **`bronze_ingest_date`**, **`bronze_run_id`**, **`ingested_at`**, then **`drop_duplicates(subset=["job_id"], keep="last")`** | Newer ingestion **wins** for the same `job_id`. |
| **History union** (used when merging) | Same **keep-last** rule after concatenating all batch Parquet files for the source | Aligns long-run Silver for Gold when merged file is stale. |

Duplicate **`job_id`** after this step is a **quality failure** (see §5).

---

## 5. Quality gate — how “dirty” rows fail loudly

After normalization and batch dedupe, **`run_silver_checks`** (`src/jmi/utils/quality.py`) runs **before** writing Parquet.

It counts:

1. **Missing title** — empty `title_norm` (or legacy `title_raw` / `title_clean` if those columns exist).  
2. **Missing company** — empty `company_norm` (or raw company fallbacks).  
3. **Duplicate `job_id`** — any duplicate in the dataframe.  
4. **Duplicate source key** — duplicate pairs of (`source`, `source_job_id`) when `source_job_id` is non-empty, else duplicate (`source`, `job_id`).

**Failure behavior:** If any check fails, `report.status != "PASS"` and **`transform_silver.run` raises `RuntimeError`** with counts. **No Silver Parquet is written** for that run (Lambda Silver step fails).

So “dirty” here means **violating Silver invariants** (missing dimensions, duplicate keys), not arbitrary noise — those are **blocked**, not imputed.

---

## 6. What is *not* treated as blocking errors

| Situation | Handling |
|-----------|----------|
| **Remote unknown** | Allowed; value **`unknown`**. |
| **Empty skills** | Allowed; stored as **`[]`**. |
| **Unparseable `posted_at`** | Allowed; **`None`**. |
| **Noisy HTML** | Stripped for downstream use; if title/company still empty after norm, **checks fail**. |

---

## 7. Outputs and audit trail

| Artifact | Content |
|----------|---------|
| **Batch Parquet** | Per-run path under `silver/jobs/source=…/ingest_date=…/run_id=…/`. |
| **Merged Parquet** | Latest snapshot path (e.g. `merged/latest.parquet`). |
| **`silver_quality_<ingest_date>_<run_id>.json`** | Row counts, dedupe stats, check pass/fail, paths (`quality_root`). |

---

## 8. Code map

| Concern | Module |
|---------|--------|
| Orchestration, dedupe, merge, fail on checks | `src/jmi/pipelines/transform_silver.py` |
| Normalization, HTML strip, location/remote/title rules, Parquet contract | `src/jmi/pipelines/silver_schema.py` |
| Skill extraction rules | `src/jmi/connectors/skill_extract.py` |
| Silver quality metrics and PASS/FAIL | `src/jmi/utils/quality.py` |

---

## 9. Summary

**Cleaning** in Silver is **deterministic normalization** (text, HTML, location shapes, remote inference rules, rule-based skills) plus **deduplication** by `job_id` with explicit **recency** ordering. **Dirty data** that breaks **referential quality** (missing title/company, duplicate keys) is **not** written: the pipeline **fails** with a **`RuntimeError`** and a **JSON quality report**, so downstream Gold and dashboards do not silently consume inconsistent rows.
