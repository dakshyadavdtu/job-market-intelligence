from __future__ import annotations

import json
from datetime import datetime, timezone

from src.jmi.arbeitnow_slice_defaults import ARBEITNOW_SLICE_DEFAULT_MIN_CREATED_AT_UTC
from src.jmi.config import AppConfig, new_run_id
from src.jmi.connectors.arbeitnow import fetch_all_jobs, job_created_at_ts, to_bronze_record
from src.jmi.paths import arbeitnow_slice_tag, bronze_raw_gz
from src.jmi.pipelines.bronze_incremental import (
    load_incremental_connector_state,
    next_fetch_watermark_epoch,
    persist_incremental_connector_ok,
    select_jobs_for_bronze,
)
from src.jmi.utils.io import ensure_dir, write_jsonl_gz


def run() -> dict:
    cfg = AppConfig()
    state = load_incremental_connector_state(cfg)
    run_id = new_run_id()
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    batch_created_at = datetime.now(timezone.utc).isoformat()

    incremental_strategy = cfg.incremental_strategy_effective()
    if incremental_strategy == "true_api_filter":
        min_param = cfg.arbeitnow_min_created_at
        slice_tag = arbeitnow_slice_tag()
        if cfg.source_name == "arbeitnow" and slice_tag:
            if min_param is None:
                min_param = ARBEITNOW_SLICE_DEFAULT_MIN_CREATED_AT_UTC
        elif min_param is None and state.fetch_watermark_created_at is not None:
            min_param = max(0, state.fetch_watermark_created_at - cfg.incremental_lookback_hours * 3600)
        raw_jobs, fetch_meta = fetch_all_jobs(
            min_created_at=min_param,
            use_min_created_at_param=True,
        )
    else:
        raw_jobs, fetch_meta = fetch_all_jobs()

    jobs_to_land, filter_diag = select_jobs_for_bronze(
        cfg, state, raw_jobs, incremental_strategy, job_created_at_ts
    )

    bronze_records = []
    for job in jobs_to_land:
        record = to_bronze_record(job)
        record["run_id"] = run_id
        record["bronze_ingest_date"] = ingest_date
        record["batch_created_at"] = batch_created_at
        bronze_records.append(record)

    out_path = bronze_raw_gz(cfg, ingest_date, run_id)
    write_jsonl_gz(out_path, bronze_records)

    new_watermark = next_fetch_watermark_epoch(raw_jobs, job_created_at_ts)

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

    slice_tag = arbeitnow_slice_tag()
    if cfg.source_name == "arbeitnow" and slice_tag:
        health_name = f"latest_ingest_arbeitnow_slice_{slice_tag}.json"
    else:
        health_name = "latest_ingest.json"
    health_path = cfg.health_root / health_name
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
                "arbeitnow_slice": slice_tag,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    state_path = persist_incremental_connector_ok(
        cfg,
        run_id=run_id,
        batch_created_at=batch_created_at,
        incremental_strategy=incremental_strategy,
        fetch_watermark_created_at=new_watermark,
    )

    return {
        "run_id": run_id,
        "record_count": len(bronze_records),
        "bronze_data_file": str(out_path),
        "manifest_file": str(manifest_path),
        "incremental_strategy": incremental_strategy,
        "connector_state_file": state_path,
        "invoke_silver": len(bronze_records) > 0,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
