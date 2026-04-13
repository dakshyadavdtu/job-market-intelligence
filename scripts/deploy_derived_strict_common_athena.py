#!/usr/bin/env python3
"""Register materialized derived/comparison/strict_common_month Parquet in Athena (jmi_analytics_v2)."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DDL = ROOT / "infra" / "aws" / "athena" / "ddl_derived_strict_common.sql"
BUCKET = os.environ.get("JMI_BUCKET", "jmi-dakshyadav-job-market-intelligence").strip() or "jmi-dakshyadav-job-market-intelligence"
OUTPUT = f"s3://{BUCKET}/athena-results/"
REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")


def run_sql(sql: str) -> str:
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
        sql,
    ]
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)["QueryExecutionId"]


def wait_done(qid: str) -> None:
    for _ in range(120):
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


def split_sql(sql: str) -> list[str]:
    parts = re.split(r";\s*\n", sql.strip())
    out: list[str] = []
    for p in parts:
        p = p.strip()
        if not p or p.startswith("--"):
            continue
        out.append(p.rstrip(";") + ";")
    return out


def main() -> int:
    body = DDL.read_text(encoding="utf-8").replace("BUCKET", BUCKET)
    stmts = split_sql(body)
    for i, s in enumerate(stmts, 1):
        print(f"Running {i}/{len(stmts)}...", flush=True)
        qid = run_sql(s)
        wait_done(qid)
        print(f"  OK {qid}", flush=True)
    print("ALL_OK", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
