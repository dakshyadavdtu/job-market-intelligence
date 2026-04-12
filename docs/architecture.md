# Architecture (MVP Phase 1)

`[Arbeitnow API | Adzuna India API] -> ingest (Lambda-compatible Python; Adzuna local today) -> Bronze (S3/local JSONL.gz, source=partition)
-> Silver transform (Lambda-compatible Python, Parquet)
-> Gold aggregate (Lambda-compatible Python, Parquet)
-> Glue Catalog metadata + Athena SQL
-> Streamlit dashboard`

Processing mode: micro-batch (target every 4 hours in AWS deployment for the primary Arbeitnow path). **Adzuna** uses the same medallion contracts locally; see `docs/adzuna_india_runbook.md`.
