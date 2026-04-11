# Data Dictionary (MVP)

## Bronze (`raw.jsonl.gz`)

- `source`: upstream source name (`arbeitnow`)
- `schema_version`: schema version string
- `job_id`: deterministic id hash
- `ingested_at`: UTC ingestion timestamp
- `raw_payload`: untouched source payload

## Silver (`jobs`)

Canonical job row (Parquet). Batch lineage: **`bronze_run_id`** matches the Bronze batch and the S3 path segment `run_id=…` (a separate column named `run_id` is not stored in the file to avoid clashing with that Hive partition key in Athena).

- `job_id`: deterministic id (stable across runs)
- `source`: source name (e.g. `arbeitnow`)
- `source_job_id`: source-native id (Arbeitnow: slug when present, else null)
- `title_raw` / `title_norm`: original title and normalized (lower, collapsed whitespace)
- `company_raw` / `company_norm`: original employer string and normalized (lower, collapsed whitespace)
- `location_raw` / `location_city` / `location_country`: raw location string; city/country split only when a comma-separated pattern is present
- `remote_type`: `remote` | `onsite` | `unknown` (from Arbeitnow `remote` boolean)
- `employment_type`: joined `job_types` when present, else null
- `category`: first non-empty Arbeitnow `tags` entry when present, else null
- `description_text`: HTML-stripped description body
- `skills`: normalized skill token list (allowlist/stoplist rules)
- `salary_min` / `salary_max` / `salary_currency`: null for Arbeitnow (not in API)
- `posted_at`: UTC posting time as `YYYY-MM-DDTHH:MM:SSZ` from `created_at` when parseable
- `ingested_at`: UTC ingest timestamp from Bronze envelope
- `record_status`: default `active`
- `raw_url`: posting URL
- `job_id_strategy`, `schema_version`, `source_record_key`: lineage / dedup helpers
- `bronze_run_id`, `bronze_ingest_date`, `bronze_data_file`: Bronze batch lineage

## Gold (`skill_demand_monthly`)

- `skill`
- `job_count`
- partition: `ingest_month`
