# Data Dictionary (MVP)

## Bronze (`raw.jsonl.gz`)

- `source`: upstream source name (`arbeitnow`)
- `schema_version`: schema version string
- `job_id`: deterministic id hash
- `ingested_at`: UTC ingestion timestamp
- `raw_payload`: untouched source payload

## Silver (`jobs`)

Canonical job row (Parquet). Batch lineage: **`bronze_run_id`** matches the Bronze batch and the S3 path segment `run_id=…` (Athena partition `run_id` is not duplicated as a separate data column in the Glue DDL).

- `job_id`, `source`, `source_job_id` (slug when present, else null)
- `title_raw`, `title_norm` — display/original vs normalized (lower, collapsed whitespace) for Gold role rollups
- `company_raw`, `company_norm` — raw employer string vs normalized for Gold company rollups
- `location_raw` — Arbeitnow location string as provided (Gold normalizes for aggregates)
- `remote_type`: `remote` | `onsite` | `unknown` from API `remote`
- `employment_type`: `job_types` from API joined with `; ` when present, else null
- `description_text`: HTML-stripped description (also used for skill phrase matching)
- `skills`: sorted unique canonical tokens from **tags + title + description** (`skill_extract`: allowlist, aliases, stoplist, phrase scan)
- `posted_at`, `ingested_at`, `raw_url`
- `job_id_strategy`, `schema_version` — audit / evolution
- `bronze_run_id`, `bronze_ingest_date`, `bronze_data_file` — batch lineage

## Gold (`skill_demand_monthly`)

- `skill`
- `job_count`
- partition: `ingest_month`
