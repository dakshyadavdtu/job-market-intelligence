from __future__ import annotations

import json
from datetime import datetime, timezone

from src.jmi.config import AppConfig, new_run_id
from src.jmi.connectors.arbeitnow import fetch_live_jobs, to_bronze_record
from src.jmi.utils.io import ensure_dir, write_jsonl_gz


def run() -> dict:
    cfg = AppConfig()
    run_id = new_run_id()
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    batch_created_at = datetime.now(timezone.utc).isoformat()

    raw_jobs = fetch_live_jobs()
    bronze_records = []
    for job in raw_jobs:
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

    manifest = {
        "source": cfg.source_name,
        "run_id": run_id,
        "bronze_ingest_date": ingest_date,
        "batch_created_at": batch_created_at,
        "record_count": len(bronze_records),
        "bronze_data_file": str(out_path),
        "schema_version": cfg.schema_version,
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
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "run_id": run_id,
        "record_count": len(bronze_records),
        "bronze_data_file": str(out_path),
        "manifest_file": str(manifest_path),
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
