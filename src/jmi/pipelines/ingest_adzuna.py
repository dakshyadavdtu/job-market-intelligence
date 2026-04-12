"""
Bronze ingest for Adzuna India (source=adzuna_in).

Requires ADZUNA_APP_ID and ADZUNA_APP_KEY (or repo-root .env / ADZUNA_ENV_FILE).
Does not invoke Silver.

Run: python -m src.jmi.pipelines.ingest_adzuna
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone

from src.jmi.config import AppConfig, new_run_id
from src.jmi.connectors import adzuna
from src.jmi.pipelines.ingest_live import _select_jobs_for_bronze
from src.jmi.utils.io import ensure_dir, write_jsonl_gz
from src.jmi.utils.source_state import ConnectorState, load_connector_state, save_connector_state


def run() -> dict:
    adzuna._bootstrap_env()
    cfg = replace(AppConfig(), source_name=adzuna.ADZUNA_SOURCE_SLUG)
    ensure_dir(cfg.state_root / f"source={cfg.source_name}")

    state = load_connector_state(cfg)
    run_id = new_run_id()
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    batch_created_at = datetime.now(timezone.utc).isoformat()

    # India: incremental via client-side watermark on ISO `created` (no Arbeitnow min_created_at API).
    incremental_strategy = "fallback_lookback"

    raw_jobs, fetch_meta = adzuna.fetch_all_jobs_india()
    jobs_to_land, filter_diag = _select_jobs_for_bronze(
        cfg, state, raw_jobs, incremental_strategy, adzuna.job_created_at_ts
    )

    bronze_records = []
    for job in jobs_to_land:
        record = adzuna.to_bronze_record(job)
        record["run_id"] = run_id
        record["bronze_ingest_date"] = ingest_date
        record["batch_created_at"] = batch_created_at
        bronze_records.append(record)

    out_path = (
        cfg.bronze_root
        / f"source={cfg.source_name}"
        / f"ingest_date={ingest_date}"
        / f"run_id={run_id}"
        / "raw.jsonl.gz"
    )
    write_jsonl_gz(out_path, bronze_records)

    ts_list = [adzuna.job_created_at_ts(j) for j in raw_jobs]
    new_watermark = max(ts_list) if ts_list else 0

    manifest = {
        "source": cfg.source_name,
        "run_id": run_id,
        "bronze_ingest_date": ingest_date,
        "batch_created_at": batch_created_at,
        "record_count": len(bronze_records),
        "bronze_data_file": str(out_path),
        "schema_version": cfg.schema_version,
        "incremental_strategy": incremental_strategy,
        "incremental_filter": filter_diag,
        "fetch_meta": fetch_meta,
        "fetch_watermark_created_at_after_run": new_watermark,
    }
    manifest_path = out_path.parent / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    health_path = cfg.health_root / "latest_ingest_adzuna_in.json"
    ensure_dir(health_path.parent)
    health_path.write_text(
        json.dumps(
            {
                "source": cfg.source_name,
                "run_id": run_id,
                "bronze_ingest_date": ingest_date,
                "batch_created_at": batch_created_at,
                "record_count": len(bronze_records),
                "bronze_path": str(out_path),
                "manifest_path": str(manifest_path),
                "incremental_strategy": incremental_strategy,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    updated = ConnectorState(
        source_name=cfg.source_name,
        last_successful_run_id=run_id,
        last_successful_run_at=batch_created_at,
        fetch_watermark_created_at=new_watermark,
        fallback_lookback_hours=cfg.incremental_lookback_hours,
        last_status="ok",
        incremental_strategy=incremental_strategy,
    )
    state_path = save_connector_state(cfg, updated)

    return {
        "run_id": run_id,
        "record_count": len(bronze_records),
        "bronze_data_file": str(out_path),
        "manifest_file": str(manifest_path),
        "incremental_strategy": incremental_strategy,
        "connector_state_file": state_path,
        "health_file": str(health_path),
        "invoke_silver": False,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
