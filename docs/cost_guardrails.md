# Cost Guardrails

- Hard project cap: `<= $3` total spend
- Serverless-only for MVP: Lambda + EventBridge + S3 + Athena + Glue Catalog metadata
- No always-on compute
- Bronze as compressed JSONL
- Silver/Gold as Parquet
- Partition pruning mandatory in Athena
- Prefer Gold-first dashboard queries
- Avoid crawlers unless absolutely needed
- Use manual DDL + controlled partition adds in MVP
- Set lifecycle retention for old Bronze snapshots
