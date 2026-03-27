# Data Dictionary (MVP)

## Bronze (`raw.jsonl.gz`)

- `source`: upstream source name (`arbeitnow`)
- `schema_version`: schema version string
- `job_id`: deterministic id hash
- `ingested_at`: UTC ingestion timestamp
- `raw_payload`: untouched source payload

## Silver (`jobs`)

- `job_id`
- `source`
- `schema_version`
- `title`
- `title_clean`
- `company_name`
- `location`
- `is_remote`
- `published_at_raw`
- `skills` (array<string>)
- `posting_url`
- `ingested_at`

## Gold (`skill_demand_monthly`)

- `skill`
- `job_count`
- partition: `ingest_month`
