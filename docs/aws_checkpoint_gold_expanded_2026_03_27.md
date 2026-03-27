# AWS checkpoint - expanded gold analytics - 2026-03-27

## Verified working run
- run_id: `20260327T154416Z-fec115ef`
- source: `arbeitnow`
- region: `ap-south-1`

## Verified pipeline status
- ingest: PASS
- silver: PASS
- gold: PASS
- athena validation: PASS

## Verified Gold outputs

### skill_demand_monthly
- row_count: `7`
- parquet: `s3://jmi-dakshyadav-job-market-intelligence/gold/skill_demand_monthly/ingest_month=2026-03/run_id=20260327T154416Z-fec115ef/part-00001.parquet`

### role_demand_monthly
- row_count: `99`
- parquet: `s3://jmi-dakshyadav-job-market-intelligence/gold/role_demand_monthly/ingest_month=2026-03/run_id=20260327T154416Z-fec115ef/part-00001.parquet`

### location_demand_monthly
- row_count: `47`
- parquet: `s3://jmi-dakshyadav-job-market-intelligence/gold/location_demand_monthly/ingest_month=2026-03/run_id=20260327T154416Z-fec115ef/part-00001.parquet`

### company_hiring_monthly
- row_count: `66`
- parquet: `s3://jmi-dakshyadav-job-market-intelligence/gold/company_hiring_monthly/ingest_month=2026-03/run_id=20260327T154416Z-fec115ef/part-00001.parquet`

## Verified Athena tables
- `jmi_gold.skill_demand_monthly`
- `jmi_gold.role_demand_monthly`
- `jmi_gold.location_demand_monthly`
- `jmi_gold.company_hiring_monthly`

## Notes
- Lambda container deployment path is working.
- S3 Gold outputs are being written correctly.
- Athena registration and validation are working for all current Gold tables.
- EventBridge schedule is still intentionally disabled.
- Current source coverage is still only `arbeitnow`.
