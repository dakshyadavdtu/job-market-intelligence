# Job Market Intelligence (JMI)

Local-first MVP pipeline for one live source (Arbeitnow), with traceable Bronze -> Silver -> Gold outputs.

## Local Execution (Exact Order)

1. Create and activate virtual environment (Python 3.11+).
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Run ingestion:
   - `python -m src.jmi.pipelines.ingest_live`
4. Run Silver transformation:
   - `python -m src.jmi.pipelines.transform_silver`
5. Run Gold transformation:
   - `python -m src.jmi.pipelines.transform_gold`
6. Run dashboard:
   - `streamlit run dashboard/app.py`

## Output Layout (Expected)

After one successful batch, expect:

- `data/bronze/source=arbeitnow/ingest_date=YYYY-MM-DD/run_id=<run_id>/raw.jsonl.gz`
- `data/bronze/source=arbeitnow/ingest_date=YYYY-MM-DD/run_id=<run_id>/manifest.json`
- `data/silver/jobs/ingest_date=YYYY-MM-DD/run_id=<run_id>/part-00001.parquet`
- `data/gold/skill_demand_monthly/ingest_month=YYYY-MM/run_id=<run_id>/part-00001.parquet`
- `data/quality/silver_quality_YYYY-MM-DD_<run_id>.json`
- `data/quality/gold_quality_YYYY-MM_<run_id>.json`
- `data/health/latest_ingest.json`

## Lineage Rules in MVP

- Bronze run and ingest date are created once during ingestion.
- Silver carries `bronze_run_id`, `bronze_ingest_date`, and `bronze_data_file`.
- Gold derives `ingest_month` from Silver/Bronze lineage (not current clock time).

## AWS Note

AWS deployment assets stay separate under `infra/aws/`; no AWS actions are executed in local MVP flow.
