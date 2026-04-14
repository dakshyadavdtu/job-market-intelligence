# Architecture (MVP Phase 1)

`[Arbeitnow API | Adzuna India API] -> ingest (Lambda-compatible Python; Adzuna local today) -> Bronze (S3/local JSONL.gz, source=partition)
-> Silver transform (Lambda-compatible Python, Parquet)
-> Gold aggregate (Lambda-compatible Python, Parquet)
-> Glue Catalog metadata + Athena SQL
-> Streamlit dashboard`

Processing mode: micro-batch (scheduled ingest in AWS uses **24-hour** cadence via `infra/aws/eventbridge/jmi-ingest-schedule.json`). **Adzuna** uses the same medallion contracts locally; see `docs/adzuna_india_runbook.md`.
