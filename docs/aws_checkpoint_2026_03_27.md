# AWS checkpoint - 2026-03-27

## Verified working run
- run_id: `20260327T154416Z-fec115ef`
- source: `arbeitnow`
- region: `ap-south-1`

## Pipeline status
- ingest: PASS
- silver: PASS
- gold: PASS
- athena gold query: PASS

## Verified outputs

### Bronze
- raw: `s3://jmi-dakshyadav-job-market-intelligence/bronze/source=arbeitnow/ingest_date=2026-03-27/run_id=20260327T154416Z-fec115ef/raw.jsonl.gz`
- manifest: `s3://jmi-dakshyadav-job-market-intelligence/bronze/source=arbeitnow/ingest_date=2026-03-27/run_id=20260327T154416Z-fec115ef/manifest.json`

### Silver
- parquet: `s3://jmi-dakshyadav-job-market-intelligence/silver/jobs/ingest_date=2026-03-27/run_id=20260327T154416Z-fec115ef/part-00001.parquet`

### Gold
- parquet: `s3://jmi-dakshyadav-job-market-intelligence/gold/skill_demand_monthly/ingest_month=2026-03/run_id=20260327T154416Z-fec115ef/part-00001.parquet`

## Verified counts
- ingest record_count: `100`
- silver row_count: `100`
- gold row_count: `7`

## Athena validation
- database: `jmi_gold`
- table: `skill_demand_monthly`
- partition:
  - ingest_month = `2026-03`
  - run_id = `20260327T154416Z-fec115ef`

## Notes
- Lambda container image deployment is working.
- S3 Bronze/Silver/Gold writes are verified.
- Athena query on Gold is verified.
- EventBridge schedule is still disabled on purpose.
