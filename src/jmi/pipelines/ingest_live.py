from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone

from src.jmi.config import AppConfig, new_run_id
from src.jmi.connectors.arbeitnow import fetch_all_jobs, job_created_at_ts, to_bronze_record
from src.jmi.utils.io import ensure_dir, write_jsonl_gz
from src.jmi.utils.source_state import ConnectorState, load_connector_state, save_connector_state


def _select_jobs_for_bronze(
    cfg: AppConfig,
    state: ConnectorState,
    raw_jobs: list[dict],
    incremental_strategy: str,
    job_created_at_ts_fn: Callable[[dict], int] | None = None,
) -> tuple[list[dict], dict]:
    """Return jobs to land in Bronze and diagnostic counters."""
    if not raw_jobs:
        raise RuntimeError("Source API returned no jobs; refusing to advance incremental state.")

    ts_fn = job_created_at_ts_fn or job_created_at_ts
    ts_list = [ts_fn(j) for j in raw_jobs]
    max_ts = max(ts_list)
    min_ts = min(ts_list)
    lookback_sec = int(cfg.incremental_lookback_hours) * 3600
    wm = state.fetch_watermark_created_at

    if incremental_strategy == "true_api_filter":
        # Case A: trust API filtering; land everything returned.
        return raw_jobs, {
            "filter_mode": "true_api_filter",
            "api_job_count": len(raw_jobs),
            "landed_job_count": len(raw_jobs),
            "filtered_out_count": 0,
            "fetch_watermark_before": wm,
            "fetch_max_created_at_observed": max_ts,
            "fetch_min_created_at_observed": min_ts,
        }

    # Case B: full fetch, client-side watermark + lookback
    if wm is None:
        return raw_jobs, {
            "filter_mode": "fallback_lookback_bootstrap",
            "api_job_count": len(raw_jobs),
            "landed_job_count": len(raw_jobs),
            "filtered_out_count": 0,
            "fetch_watermark_before": None,
            "fetch_max_created_at_observed": max_ts,
            "fetch_min_created_at_observed": min_ts,
        }

    cutoff = wm - lookback_sec
    selected = [j for j in raw_jobs if ts_fn(j) > cutoff]
    return selected, {
        "filter_mode": "fallback_lookback",
        "api_job_count": len(raw_jobs),
        "landed_job_count": len(selected),
        "filtered_out_count": len(raw_jobs) - len(selected),
        "fetch_watermark_before": wm,
        "fetch_cutoff_created_at_exclusive": cutoff,
        "fetch_max_created_at_observed": max_ts,
        "fetch_min_created_at_observed": min_ts,
    }


def run() -> dict:
    cfg = AppConfig()
    state = load_connector_state(cfg)
    run_id = new_run_id()
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    batch_created_at = datetime.now(timezone.utc).isoformat()

    incremental_strategy = cfg.incremental_strategy_default
    if incremental_strategy == "true_api_filter":
        min_param = cfg.arbeitnow_min_created_at
        if min_param is None and state.fetch_watermark_created_at is not None:
            min_param = max(0, state.fetch_watermark_created_at - cfg.incremental_lookback_hours * 3600)
        raw_jobs, fetch_meta = fetch_all_jobs(
            min_created_at=min_param,
            use_min_created_at_param=True,
        )
    else:
        raw_jobs, fetch_meta = fetch_all_jobs()

    jobs_to_land, filter_diag = _select_jobs_for_bronze(cfg, state, raw_jobs, incremental_strategy)

    bronze_records = []
    for job in jobs_to_land:
        record = to_bronze_record(job)
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

    ts_list = [job_created_at_ts(j) for j in raw_jobs]
    new_watermark = max(ts_list)

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

    health_path = cfg.health_root / "latest_ingest.json"
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

    result = {
        "run_id": run_id,
        "record_count": len(bronze_records),
        "bronze_data_file": str(out_path),
        "manifest_file": str(manifest_path),
        "incremental_strategy": incremental_strategy,
        "connector_state_file": state_path,
        "invoke_silver": len(bronze_records) > 0,
    }
    return result


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
