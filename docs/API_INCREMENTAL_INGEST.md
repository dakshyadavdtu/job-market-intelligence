# API-side incremental ingest

This document explains how **incremental updates from the job APIs** are handled in JMI: **what is fetched**, **what is written to Bronze**, and **how state advances** between runs. Implementation is shared in **`src/jmi/pipelines/bronze_incremental.py`** and **`src/jmi/utils/source_state.py`**; **Arbeitnow** uses **`src/jmi/pipelines/ingest_live.py`**; **Adzuna India** uses **`src/jmi/pipelines/ingest_adzuna.py`**.

---

## 1. Where state lives

After each **successful** ingest, connector state is saved under **`JMI_DATA_ROOT`**:

```text
state/source=<slug>/connector_state.json
```

If **`JMI_ARBEITNOW_SLICE`** is set for Arbeitnow:

```text
state/source=arbeitnow/slice=<tag>/connector_state.json
```

**`ConnectorState`** (JSON) stores at least:

| Field | Meaning |
|-------|---------|
| **`fetch_watermark_created_at`** | Unix seconds — **high-water mark** of job `created` / `created_at` seen in the **last full API response** (not only rows landed in Bronze). |
| **`last_successful_run_id`** / **`last_successful_run_at`** | Audit pointers to the last good batch. |
| **`incremental_strategy`** | Which algorithm was used (`true_api_filter` or `fallback_lookback`). |
| **`fallback_lookback_hours`** | Copied from config (`JMI_INCREMENTAL_LOOKBACK_HOURS`, default **48**). |

If the file is missing or unreadable, ingest **bootstraps** with **`fetch_watermark_created_at = null`** (first run behavior).

---

## 2. Two incremental strategies

**`AppConfig.incremental_strategy_effective()`** (`src/jmi/config.py`):

- **Arbeitnow:** Uses **`incremental_strategy_default`**: if **`JMI_ARBEITNOW_USE_MIN_CREATED_AT_PARAM`** is enabled **and** **`JMI_ARBEITNOW_MIN_CREATED_AT`** is set → **`true_api_filter`**; otherwise **`fallback_lookback`**.
- **Adzuna (`adzuna_in`):** Always **`fallback_lookback`** (no API `min_created_at` path in the Adzuna connector used by the ingest module).

---

## 3. Strategy A: `true_api_filter` (Arbeitnow only)

**When:** `JMI_ARBEITNOW_USE_MIN_CREATED_AT_PARAM` + `JMI_ARBEITNOW_MIN_CREATED_AT` (Unix seconds), or slice defaults that supply a floor.

**Fetch:** `fetch_all_jobs(..., min_created_at=<param>, use_min_created_at_param=True)` adds **`min_created_at`** to the Arbeitnow HTTP query (`src/jmi/connectors/arbeitnow.py`). The API returns **already-filtered** rows.

**Bronze selection:** `select_jobs_for_bronze` **does not** drop rows for time — it returns **all** `raw_jobs` returned by the API (`filter_mode: true_api_filter`).

**Watermark:** `next_fetch_watermark_epoch` sets the next watermark to **`max(created_at)` over the entire API response** (full fetch), so the next run’s floor can move forward even if you only land a subset.

---

## 4. Strategy B: `fallback_lookback` (default Arbeitnow path; always Adzuna)

**Fetch:**

- **Arbeitnow:** `fetch_all_jobs()` **without** `min_created_at` (full paginated snapshot up to the configured **page cap**).
- **Adzuna:** `fetch_all_jobs_india()` — paginated India search; **no** server-side “since last run” parameter in the same way as Arbeitnow’s `min_created_at`.

So the API often returns **many jobs**; **client-side** filtering decides what lands in Bronze.

**Bronze selection** (`select_jobs_for_bronze`):

1. **Empty API response** → **`RuntimeError`** (ingest refuses to advance; no empty Bronze batch from “no data”).
2. **`fetch_watermark_created_at is None`** (bootstrap): **land every job** in the response (`filter_mode: fallback_lookback_bootstrap`).
3. **Watermark exists:**  
   - Compute **`cutoff = watermark - (incremental_lookback_hours × 3600)`** (exclusive lower bound: keep jobs with **`created_at > cutoff`**).  
   - Optional **`JMI_BRONZE_INCREMENTAL_CUTOFF_CAP_TS`**: **`cutoff = min(cutoff, cap_ts)`** so you can widen what lands (e.g. include older months still present in the API response). Documented in `bronze_incremental.py` header.

**Watermark:** Still **`max(created_at)` over the full `raw_jobs` list** — even rows **filtered out** of Bronze still advance the watermark. That avoids re-fetching the same tail of the API snapshot forever when the API keeps returning old listings.

---

## 5. End-to-end flow per run

```
1. load_incremental_connector_state(state/source=…/connector_state.json)
2. Fetch jobs from API (strategy-dependent query params)
3. select_jobs_for_bronze → jobs_to_land + filter_diag
4. Write Bronze raw.jsonl.gz + manifest.json (incremental_filter in manifest)
5. new_watermark = max(created_at) on full API response
6. persist_incremental_connector_ok(..., fetch_watermark_created_at=new_watermark)
```

**Manifest** (`manifest.json` next to Bronze) records **`incremental_strategy`**, **`incremental_filter`** (diagnostics), and **`fetch_watermark_created_at_after_run`** for audit.

---

## 6. Silver / Gold vs API incrementals

- **Silver** dedupes by **`job_id`** and merges history; it does **not** re-call the API. Incremental behavior at Bronze **reduces duplicate landing** of very old rows when the API keeps returning them.
- **Gold** has a **separate** “incremental” concept (env **`JMI_GOLD_INCREMENTAL_POSTED_MONTHS`**, etc.) controlling **which `posted_month` partitions** to rewrite — that is **downstream of Silver**, not the API fetch policy.

---

## 7. Environment variables (reference)

| Variable | Role |
|----------|------|
| **`JMI_INCREMENTAL_LOOKBACK_HOURS`** | Hours for `fallback_lookback` cutoff window (default **48**). |
| **`JMI_ARBEITNOW_USE_MIN_CREATED_AT_PARAM`** + **`JMI_ARBEITNOW_MIN_CREATED_AT`** | Enable **`true_api_filter`** for Arbeitnow. |
| **`JMI_BRONZE_INCREMENTAL_CUTOFF_CAP_TS`** | Optional Unix cap on cutoff (widen what lands vs strict lookback). |
| **`JMI_ARBEITNOW_SLICE`** | Separate **`connector_state.json`** per slice for isolated Arbeitnow runs. |

---

## 8. Code map

| Concern | File |
|---------|------|
| Shared selection + watermark logic | `src/jmi/pipelines/bronze_incremental.py` |
| State JSON path + load/save | `src/jmi/utils/source_state.py` |
| Strategy flags | `src/jmi/config.py` (`incremental_strategy_effective`, `incremental_strategy_default`) |
| Arbeitnow HTTP + pagination | `src/jmi/connectors/arbeitnow.py` |
| Arbeitnow ingest orchestration | `src/jmi/pipelines/ingest_live.py` |
| Adzuna ingest orchestration | `src/jmi/pipelines/ingest_adzuna.py` |

---

## 9. Summary

**Incremental API handling** means: **persist a per-source watermark** (max observed `created` time), **fetch** either with an API-side floor (**Arbeitnow `min_created_at`**) or a **full snapshot**, then **optionally filter client-side** to a sliding window behind the watermark, while **always advancing the watermark from the full response** so repeated runs make progress. **Bootstrap** runs land everything the API returned. **Adzuna** uses only the **client lookback** path; **Arbeitnow** can use **API filter** or **lookback** depending on environment variables.
