# Adzuna India (`adzuna_in`) — local runbook & handoff

## What this source is

- **Adzuna** job-search API for **India** (`country=in`), used as a **second live postings source** alongside **Arbeitnow** (Europe-oriented).
- **Repo source slug:** `adzuna_in` (Bronze `source=adzuna_in`, Silver/Gold `source` column).
- **Connector:** `src/jmi/connectors/adzuna.py` (fetch, inspect CLI, Bronze envelope helpers).
- **Pipelines:** `ingest_adzuna` → `transform_silver --source adzuna_in` → `transform_gold --source adzuna_in`.

## Credentials (local only — never commit)

| Variable | Required | Purpose |
|----------|----------|---------|
| `ADZUNA_APP_ID` | Yes | Adzuna developer app id |
| `ADZUNA_APP_KEY` | Yes | Adzuna developer app key |

Optional:

| Variable | Default | Purpose |
|----------|---------|---------|
| `ADZUNA_ENV_FILE` | — | Path to a `KEY=value` file (loads if env vars unset) |
| Repo-root `.env` | — | Same format; gitignored |
| `JMI_ADZUNA_MAX_PAGES` | `3` | API pages per ingest (max 50 jobs/page) |

Register keys at [developer.adzuna.com](https://developer.adzuna.com/).

## One clean end-to-end local run

From repo root, with a venv and `pip install -r requirements.txt`:

```bash
cd /path/to/job-market-intelligence-main
source .venv/bin/activate   # or: .venv/bin/python …

export ADZUNA_APP_ID="your_app_id"
export ADZUNA_APP_KEY="your_app_key"

# 1) Bronze (optional: limit pages for a quick test)
JMI_ADZUNA_MAX_PAGES=2 python -m src.jmi.pipelines.ingest_adzuna

# 2) Silver
python -m src.jmi.pipelines.transform_silver --source adzuna_in

# 3) Gold (does not overwrite gold/latest_run_metadata — Arbeitnow pointer preserved)
python -m src.jmi.pipelines.transform_gold --source adzuna_in
```

**Inspect / debug API only (no Bronze write):**

```bash
python -m src.jmi.connectors.adzuna --results-per-page 20
python -m src.jmi.connectors.adzuna --fixture   # doc-shaped JSON, no API
```

## Expected outputs (paths)

- **Bronze:** `data/bronze/source=adzuna_in/ingest_date=YYYY-MM-DD/run_id=<run_id>/raw.jsonl.gz` + `manifest.json`
- **Health:** `data/health/latest_ingest_adzuna_in.json`
- **State:** `data/state/source=adzuna_in/connector_state.json`
- **Silver:** `data/silver/jobs/source=adzuna_in/ingest_date=.../run_id=.../part-00001.parquet` and `.../merged/latest.parquet`
- **Quality:** `data/quality/silver_quality_<ingest_date>_<run_id>.json`
- **Gold:** `data/gold/{skill_demand_monthly,role_demand_monthly,location_demand_monthly,company_hiring_monthly,pipeline_run_summary}/ingest_month=YYYY-MM/run_id=<run_id>/part-00001.parquet`
- **Quality:** `data/quality/gold_quality_<run_id>.json`

Row counts depend on fetch size and **incremental state** (repeat runs may land only jobs newer than the watermark). **Gold** reads **`merged/latest.parquet`**, so aggregates for an `ingest_month` include **all** merged Silver rows in that month—not only the last Bronze batch. **pipeline_run_summary** should show `status: PASS`.

**One-liner (from repo root):** `scripts/adzuna_e2e_local.sh` (same env vars as above).

## Known limitations (expect these in analytics)

1. **Salary:** Many India rows have **no** `salary_min` / `salary_max` on the job object; salary is **not** in the Silver contract today (same as Arbeitnow MVP).
2. **Skills:** No Arbeitnow-style **tags**; skills come from **title + description** rules only → often **weaker / sparser** than Arbeitnow.
3. **Nested payload:** `company`, `location`, `category` are **objects** in Bronze; Silver flattens what the contract needs; category is **not** a Silver column in this phase.
4. **Remote:** Adzuna payload has **no** explicit remote flag → Silver `remote_type` is **`unknown`**.
5. **Roles in Gold:** `role_demand_monthly` is keyed on **`title_norm`** → high cardinality for diverse titles (many one-job “roles”).
6. **`latest_run_metadata`:** Adzuna Gold runs **do not** update `data/gold/latest_run_metadata/` so existing **Athena `latest_pipeline_run` views** stay tied to **Arbeitnow** until you add a multi-source strategy.

## Not done in this repo phase (live AWS / BI)

- **Lambda / EventBridge:** Deployed ingest is still **Arbeitnow-oriented**; Adzuna is **manual/local** unless you add a second handler or parameterize.
- **Glue / Athena:** No DDL change required to **read** Adzuna Gold files if you **upload** under the same table prefixes with new `run_id` partitions — but **views** that filter on `latest_run_metadata` will **ignore** Adzuna until extended.
- **QuickSight:** No dataset or visual changes; add **source** / **run_id** parameters when you want India side-by-side.

## Recommended next live step (do separately, with approval)

**Priority:** Extend **Athena analytics layer** (and optionally Glue projection ranges) so dashboards can query Adzuna partitions **explicitly**:

1. **Upload** local Adzuna Gold Parquet to S3 under the existing Gold prefixes (same layout as Arbeitnow).
2. **MSCK / partition projection:** Ensure `ingest_month` and new `run_id` values are visible (same as any new batch).
3. **Views:** Add `source = 'adzuna_in'` (and chosen `run_id`) to `jmi_analytics` views **or** add a parallel `latest_run_metadata_adzuna` + `latest_pipeline_run_adzuna` pattern — **before** mixing into one QuickSight page without filters.

Secondary: **dashboard** — add a **source** control and duplicate or parameterized datasets for India vs Europe.

---

*Last aligned with repo pipelines: Bronze → Silver → Gold for `adzuna_in`.*
