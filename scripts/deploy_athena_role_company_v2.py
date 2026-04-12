#!/usr/bin/env python3
"""Deploy ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql to jmi_analytics_v2 (one-time)."""
from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from pathlib import Path

ACCOUNT = "470441577506"
REGION = "ap-south-1"
OUTPUT = "s3://jmi-dakshyadav-job-market-intelligence/athena-results/"
WORKGROUP = "primary"


def strip_line_comments(sql: str) -> str:
    out: list[str] = []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        out.append(line)
    return "\n".join(out).strip()


def split_views(sql: str) -> list[str]:
    lines = sql.splitlines()
    blocks: list[str] = []
    cur: list[str] = []
    for line in lines:
        if re.match(r"^\s*CREATE\s+OR\s+REPLACE\s+VIEW\b", line) and cur:
            blocks.append(strip_line_comments("\n".join(cur)))
            cur = [line]
        else:
            cur.append(line)
    if cur:
        blocks.append(strip_line_comments("\n".join(cur)))
    return [b for b in blocks if b]


def run(sql: str, db: str) -> None:
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
        f"Database={db}",
    ]
    out = subprocess.check_output(cmd, text=True)
    qid = json.loads(out)["QueryExecutionId"]
    for _ in range(180):
        raw = subprocess.check_output(
            [
                "aws",
                "athena",
                "get-query-execution",
                "--region",
                REGION,
                "--query-execution-id",
                qid,
            ],
            text=True,
        )
        st = json.loads(raw)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            print(f"OK {qid}", file=sys.stderr)
            return
        if st in ("FAILED", "CANCELLED"):
            reason = json.loads(raw)["QueryExecution"].get("Status", {}).get(
                "StateChangeReason", ""
            )
            raise RuntimeError(f"{qid} {st}: {reason}")
        time.sleep(1)
    raise TimeoutError(qid)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    path = root / "docs" / "dashboard_implementation" / "ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql"
    raw = path.read_text(encoding="utf-8")
    raw = raw.replace("jmi_analytics.", "jmi_analytics_v2.")
    raw = raw.replace("jmi_gold.", "jmi_gold_v2.")
    for stmt in split_views(raw):
        print("Running view...", file=sys.stderr)
        run(stmt, "jmi_analytics_v2")
    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
