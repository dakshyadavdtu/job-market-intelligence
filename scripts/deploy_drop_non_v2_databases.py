#!/usr/bin/env python3
"""Run Athena DROP DATABASE CASCADE for jmi_analytics, jmi_gold, jmi_silver (non-v2 only)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
WORKGROUP = "primary"
BUCKET = os.environ.get("JMI_BUCKET", "jmi-dakshyadav-job-market-intelligence").strip() or "jmi-dakshyadav-job-market-intelligence"
OUTPUT = f"s3://{BUCKET}/athena-results/"
ROOT = Path(__file__).resolve().parents[1]

STATEMENTS = [
    "DROP DATABASE IF EXISTS jmi_analytics CASCADE",
    "DROP DATABASE IF EXISTS jmi_gold CASCADE",
    "DROP DATABASE IF EXISTS jmi_silver CASCADE",
]


def _run_one(stmt: str) -> str:
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
    ]
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)["QueryExecutionId"]


def _wait(qid: str) -> None:
    for _ in range(180):
        raw = subprocess.check_output(
            ["aws", "athena", "get-query-execution", "--region", REGION, "--query-execution-id", qid],
            text=True,
        )
        st = json.loads(raw)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            return
        if st in ("FAILED", "CANCELLED"):
            reason = json.loads(raw)["QueryExecution"].get("Status", {}).get("StateChangeReason", "")
            raise RuntimeError(f"{qid} {st}: {reason}")
        time.sleep(1)
    raise TimeoutError(qid)


def main() -> int:
    for stmt in STATEMENTS:
        print(f"Running: {stmt}", file=sys.stderr)
        qid = _run_one(stmt)
        _wait(qid)
        print(f"OK {qid}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
