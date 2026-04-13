#!/usr/bin/env python3
"""Smoke checks for jmi_silver_v2 + v2_in_silver_jobs_* foundation (Athena)."""
from __future__ import annotations

import json
import subprocess
import sys
import time

REGION = "ap-south-1"
WORKGROUP = "primary"
OUTPUT = "s3://jmi-dakshyadav-job-market-intelligence/athena-results/"

QUERIES: list[tuple[str, str]] = [
    ("base_ct", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_silver_jobs_base"),
    ("skills_long_ct", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_silver_jobs_skills_long"),
    (
        "describe_base",
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'jmi_analytics_v2' AND table_name = 'v2_in_silver_jobs_base' ORDER BY ordinal_position",
    ),
]


def run_one(sql: str) -> str:
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
        "--query-execution-context",
        "Database=jmi_analytics_v2",
    ]
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)["QueryExecutionId"]


def wait(qid: str) -> None:
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


def main() -> int:
    for name, sql in QUERIES:
        qid = run_one(sql)
        wait(qid)
        print(f"OK\t{name}\t{qid}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
