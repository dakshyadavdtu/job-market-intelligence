#!/usr/bin/env python3
"""Run Athena DROP for legacy jmi_silver.jobs (flat Silver layout)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
WORKGROUP = "primary"
BUCKET = os.environ.get("JMI_BUCKET", "jmi-dakshyadav-job-market-intelligence").strip() or "jmi-dakshyadav-job-market-intelligence"
OUTPUT = f"s3://{BUCKET}/athena-results/"
ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    sql = (ROOT / "infra" / "aws" / "athena" / "drop_legacy_jmi_silver_flat_table.sql").read_text(encoding="utf-8")
    lines = [ln for ln in sql.splitlines() if ln.strip() and not ln.strip().startswith("--")]
    stmt = "\n".join(lines).strip()
    print("Deploying DROP legacy jmi_silver.jobs ...", file=sys.stderr)
    cmd = [
        "aws",
        "athena",
        "start-query-execution",
        "--region",
        REGION,
        "--work-group",
        WORKGROUP,
        "--result-configuration",
        f"OutputLocation={OUTPUT}",
        "--query-string",
        stmt,
        "--query-execution-context",
        "Database=jmi_silver",
    ]
    out = subprocess.check_output(cmd, text=True)
    qid = json.loads(out)["QueryExecutionId"]
    for _ in range(120):
        raw = subprocess.check_output(
            ["aws", "athena", "get-query-execution", "--region", REGION, "--query-execution-id", qid],
            text=True,
        )
        st = json.loads(raw)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            print(f"OK {qid}", file=sys.stderr)
            return 0
        if st in ("FAILED", "CANCELLED"):
            reason = json.loads(raw)["QueryExecution"].get("Status", {}).get("StateChangeReason", "")
            raise RuntimeError(f"{qid} {st}: {reason}")
        import time

        time.sleep(1)
    raise TimeoutError(qid)


if __name__ == "__main__":
    raise SystemExit(main())
