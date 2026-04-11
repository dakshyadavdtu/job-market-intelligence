# Architecture (MVP Phase 1)

`Arbeitnow API -> ingest (Lambda-compatible Python) -> Bronze (S3/local JSONL.gz)
-> Silver transform (Lambda-compatible Python, Parquet)
-> Gold aggregate (Lambda-compatible Python, Parquet)
-> Glue Catalog metadata + Athena SQL
-> Streamlit dashboard`

Processing mode: micro-batch (target every 10 minutes in AWS deployment).
