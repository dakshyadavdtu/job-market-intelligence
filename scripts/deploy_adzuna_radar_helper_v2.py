#!/usr/bin/env python3
"""Deploy jmi_analytics_v2.v2_in_radar_profile_monthly."""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

REGION = "ap-south-1"
WORKGROUP = "primary"
BUCKET = "jmi-dakshyadav-job-market-intelligence"
OUTPUT = f"s3://{BUCKET}/athena-results/"
ROOT = Path(__file__).resolve().parents[1]


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
        "--query-execution-context",
        "Database=jmi_analytics_v2",
    ]
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)["QueryExecutionId"]


def wait(qid: str) -> None:
    for _ in range(300):
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
    path = ROOT / "infra" / "aws" / "athena" / "analytics_v2_adzuna_radar_helper.sql"
    raw = path.read_text(encoding="utf-8")
    lines = []
    for line in raw.splitlines():
        if lines or line.strip().startswith("CREATE"):
            lines.append(line)
    stmt = "\n".join(lines).strip()
    print("Deploying radar profile view...", file=sys.stderr)
    qid = run_sql(stmt)
    wait(qid)
    print(f"  OK {qid}", file=sys.stderr)
    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
