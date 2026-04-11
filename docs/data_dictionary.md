# Data Dictionary (MVP)

## Bronze (`raw.jsonl.gz`)

- `source`, `schema_version`, `job_id`, `job_id_strategy`, `ingested_at`, `raw_payload`, batch fields (`run_id`, `bronze_ingest_date`, …)

## Silver (`jobs`)

Minimal columns: Gold inputs, operational lineage, and job facts not duplicated unnecessarily from Bronze.

- `job_id`, `source`, `source_job_id` (slug when present)
- `title_norm`, `company_norm` — normalized for Gold role/company rollups (full title/company text lives in Bronze `raw_payload`)
- `location_raw` — Gold applies location normalization
- `remote_type`, `employment_type` — Arbeitnow facts
- `skills` — rule-based extraction + **source tag fallback** when allowlist yields nothing (tags are API-native, not invented)
- `posted_at`, `ingested_at`
- `job_id_strategy` — how `job_id` was derived (audit)
- `bronze_run_id`, `bronze_ingest_date`, `bronze_data_file` — batch lineage

Long text (`description`), display casing (`title`/`company_name` as returned), URLs, and `schema_version` remain on **Bronze** only.

## Gold (`skill_demand_monthly`)

- `skill`, `job_count`, partition `ingest_month`
