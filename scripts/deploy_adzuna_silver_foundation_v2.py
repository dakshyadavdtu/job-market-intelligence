#!/usr/bin/env python3
"""Deploy jmi_silver_v2.adzuna_jobs_merged + jmi_analytics_v2 Adzuna Silver foundation views."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REGION = "ap-south-1"
WORKGROUP = "primary"
BUCKET = "jmi-dakshyadav-job-market-intelligence"
OUTPUT = f"s3://{BUCKET}/athena-results/"
ROOT = Path(__file__).resolve().parents[1]


def run_sql(sql: str, database: str | None) -> str:
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
    if database:
        cmd.extend(["--query-execution-context", f"Database={database}"])
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
        import time

        time.sleep(1)
    raise TimeoutError(qid)


def split_statements(sql_text: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    for line in sql_text.splitlines():
        s = line.strip()
        if s.upper().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(buf).strip()
            if stmt and stmt != ";":
                parts.append(stmt.rstrip(";").strip() + ";")
            buf = []
    if buf:
        stmt = "\n".join(buf).strip()
        if stmt:
            parts.append(stmt.rstrip(";").strip() + ";")
    return parts


def main() -> int:
    ddl_path = ROOT / "infra" / "aws" / "athena" / "ddl_silver_v2_adzuna_merged.sql"
    views_path = ROOT / "infra" / "aws" / "athena" / "analytics_v2_adzuna_silver_foundation.sql"

    ddl_text = ddl_path.read_text(encoding="utf-8")
    ddl_stmts = [s for s in split_statements(ddl_text) if s.strip()]

    print("Deploying jmi_silver_v2.adzuna_jobs_merged...", file=sys.stderr)
    for stmt in ddl_stmts:
        su = stmt.upper().strip()
        if su.startswith("CREATE DATABASE"):
            db = None
        elif su.startswith("CREATE EXTERNAL TABLE"):
            db = "jmi_silver_v2"
        else:
            db = None
        qid = run_sql(stmt, db)
        wait(qid)
        print(f"  OK {qid}", file=sys.stderr)

    views_text = views_path.read_text(encoding="utf-8")
    view_stmts = [s for s in split_statements(views_text) if s.strip()]

    print("Deploying jmi_analytics_v2 foundation views...", file=sys.stderr)
    for stmt in view_stmts:
        qid = run_sql(stmt, "jmi_analytics_v2")
        wait(qid)
        print(f"  OK {qid}", file=sys.stderr)

    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
