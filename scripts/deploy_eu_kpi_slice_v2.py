#!/usr/bin/env python3
"""Deploy jmi_analytics_v2 EU KPI slice + lightweight DQ helper views."""
from __future__ import annotations

import json
import re
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


def split_create_statements(sql_text: str) -> list[str]:
    """Split file on ';' before CREATE OR REPLACE (multiple views per file)."""
    text = sql_text.strip()
    if not text:
        return []
    parts = re.split(r";\s*(?=CREATE\s+OR\s+REPLACE\s+VIEW)", text, flags=re.IGNORECASE | re.DOTALL)
    return [p.strip().rstrip(";").strip() + ";" for p in parts if p.strip()]


def deploy_file(path: Path, label: str) -> None:
    raw = path.read_text(encoding="utf-8")
    stmts = split_create_statements(raw)
    if len(stmts) == 1 and not re.search(r"CREATE\s+OR\s+REPLACE", raw, re.I):
        stmts = [raw.strip().rstrip(";").strip() + ";"]
    print(f"Deploying {label} ({len(stmts)} statement(s))...", file=sys.stderr)
    for i, stmt in enumerate(stmts, 1):
        qid = run_sql(stmt)
        wait(qid)
        print(f"  OK {i}/{len(stmts)} {qid}", file=sys.stderr)


def main() -> int:
    deploy_file(ROOT / "infra" / "aws" / "athena" / "analytics_v2_eu_kpi_slice.sql", "v2_eu_kpi_slice_monthly")
    deploy_file(ROOT / "infra" / "aws" / "athena" / "analytics_v2_eu_dq_helpers.sql", "EU DQ helpers")
    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
