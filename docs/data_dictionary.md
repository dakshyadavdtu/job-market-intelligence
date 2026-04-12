# Data Dictionary (MVP)

## Bronze (`raw.jsonl.gz`)

- `source`, `schema_version`, `job_id`, `job_id_strategy`, `ingested_at`, `raw_payload`, batch fields (`run_id`, `bronze_ingest_date`, …)
- **Adzuna India (`adzuna_in`):** also `source_job_id` (vendor `id`), `source_slug` (same id for parity), `source_url` (`redirect_url`). `raw_payload` is the full Adzuna job object (nested `company` / `location` / `category`, ISO `created`).

## Silver (`jobs`)

Strict contract: Parquet files contain **only** the columns below (no legacy salary/location_country/record_status/schema_version/source_record_key, etc.).

- `job_id`, `source`, `source_job_id` (slug when present)
- `title_norm`, `company_norm` — Gold role/company rollups (full strings on Bronze `raw_payload`)
- `location_raw` — `normalize_location_raw` in Silver (comma trim/dedupe); Gold uses the same helper for aggregates
- `remote_type` — Arbeitnow `remote` flag mapped to `remote` / `onsite` / `unknown`; **Adzuna India** → `unknown` (no vendor remote flag in payload)
- `posted_at` — Arbeitnow from Unix `created_at`; **Adzuna** from ISO string `created` in `raw_payload`
- `skills` — rule-based extraction + API tag fallback
- `posted_at`, `ingested_at`
- `job_id_strategy` — audit of `job_id` derivation
- `bronze_run_id`, `bronze_ingest_date`, `bronze_data_file` — batch lineage

`job_types`, long description, salary, display title casing, URL, `schema_version` live on **Bronze** only.

## Gold (`skill_demand_monthly`)

- `skill`, `job_count`, partition `ingest_month`

**Multi-source:** Fact tables include `source` (e.g. `adzuna_in`). Run Gold with `--source adzuna_in` to build partitions from that source’s merged Silver. Each Gold run updates **that source’s** pointer Parquet only: **`gold/source=arbeitnow/latest_run_metadata/`** for default/EU runs and **`gold/source=adzuna_in/latest_run_metadata/`** for Adzuna runs (no cross-source overwrite). Athena **`jmi_gold.latest_run_metadata`** remains EU-oriented via Glue **LOCATION** under the Arbeitnow prefix; Adzuna uses **`latest_run_metadata_adzuna`** (see repo DDL).
