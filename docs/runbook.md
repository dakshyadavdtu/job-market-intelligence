# JMI Runbook (MVP)

## Local Build Actions (No AWS Changes)

1. Install dependencies: `pip install -r requirements.txt`
2. Run ingestion: `python -m src.jmi.pipelines.ingest_live`
3. Run silver transform: `python -m src.jmi.pipelines.transform_silver`
4. Run gold transform: `python -m src.jmi.pipelines.transform_gold`
5. Start dashboard: `streamlit run dashboard/app.py`

## AWS Deployment Actions (Execute only with explicit approval)

Order of operations:
1. Account safety (MFA, dedicated account/profile)
2. Budget alarms and hard guardrails
3. S3 bucket and Bronze/Silver/Gold prefixes
4. IAM least-privilege roles for Lambda and Athena
5. Lambda functions (ingest/transform)
6. EventBridge schedule (10-minute cadence)
7. Glue Data Catalog + Athena DDL
8. Manual validation batch
9. Dashboard verification

Before any AWS action, document:
- What changes are being made
- Why required for MVP
- Cost impact vs `$3` cap
- Rollback and teardown steps
