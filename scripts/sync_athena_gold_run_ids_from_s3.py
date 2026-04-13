#!/usr/bin/env python3
"""Merge S3 gold run_ids into jmi_gold_v2 partition projection (Glue TBLPROPERTIES)."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

BUCKET = os.environ.get("JMI_BUCKET", "jmi-dakshyadav-job-market-intelligence").strip()
REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"


def _load_deploy():
    import importlib.util

    path = ROOT / "scripts" / "deploy_athena_v2.py"
    spec = importlib.util.spec_from_file_location("deploy_athena_v2", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    deploy_athena_v2 = _load_deploy()

    out = subprocess.check_output(
        [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            BUCKET,
            "--prefix",
            "gold/role_demand_monthly/",
            "--region",
            REGION,
            "--query",
            "Contents[].Key",
            "--output",
            "text",
        ],
        text=True,
    )
    keys = [k for k in out.replace("\t", "\n").splitlines() if k.strip()]
    run_ids: set[str] = set()
    for k in keys:
        if "run_id=" in k:
            part = k.split("run_id=", 1)[1].split("/", 1)[0]
            run_ids.add(part)
    if not run_ids:
        print("No run_ids found under gold/role_demand_monthly/", file=sys.stderr)
        return 1
    csv = ",".join(sorted(run_ids))
    deploy_athena_v2.update_gold_v2_run_id_projection(csv, region=REGION, workgroup="primary")
    print(f"Updated projection with {len(run_ids)} run_id(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
