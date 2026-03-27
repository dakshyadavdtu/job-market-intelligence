# Job Market Intelligence (JMI)

Serverless-first, ultra-low-cost job market intelligence pipeline designed for AWS deployment and local-first MVP development.

## MVP Scope (Phase 1)

- One live source connector (Arbeitnow API)
- Raw Bronze storage (JSONL snapshots)
- Silver standardized dataset (Parquet)
- Gold aggregated dataset (Parquet)
- Streamlit dashboard (reads Gold locally for MVP)
- Data quality and freshness metadata

## Local Quick Start

1. Create Python 3.11+ virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Run one full local micro-batch:
   - `python -m src.jmi.pipelines.ingest_live`
   - `python -m src.jmi.pipelines.transform_silver`
   - `python -m src.jmi.pipelines.transform_gold`
4. Start dashboard:
   - `streamlit run dashboard/app.py`

## Data Lake Layout (Local MVP)

- `data/bronze/source=arbeitnow/ingest_date=YYYY-MM-DD/run_id=<run_id>/raw.jsonl.gz`
- `data/silver/jobs/ingest_date=YYYY-MM-DD/part-*.parquet`
- `data/gold/skill_demand_monthly/ingest_month=YYYY-MM/part-*.parquet`
- `data/quality/` and `data/health/` for checks and freshness metadata

## AWS Deployment Notes

AWS resource creation is intentionally separated. See:
- `infra/aws/README.md`
- `docs/runbook.md`
