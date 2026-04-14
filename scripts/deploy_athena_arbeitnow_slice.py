#!/usr/bin/env python3
"""Create/update Glue tables for Arbeitnow slice Gold (jmi_gold_arbeitnow_slice) and sync run_id projection from S3."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.jmi.aws.athena_projection import sync_arbeitnow_slice_gold_projection_from_s3  # noqa: E402


def athena_bucket() -> str:
    return os.environ.get("JMI_BUCKET", "jmi-dakshyadav-job-market-intelligence").strip() or "jmi-dakshyadav-job-market-intelligence"


def output_uri() -> str:
    return f"s3://{athena_bucket()}/athena-results/"


def run_sql(sql: str, *, region: str, workgroup: str, database: str | None) -> None:
    cmd = [
        "aws",
        "athena",
        "start-query-execution",
        "--region",
        region,
        "--work-group",
        workgroup,
        "--result-configuration",
        f"OutputLocation={output_uri()}",
        "--query-string",
        sql,
    ]
    if database:
        cmd.extend(["--query-execution-context", f"Database={database}"])
    out = subprocess.check_output(cmd, text=True)
    qid = json.loads(out)["QueryExecutionId"]
    for _ in range(180):
        raw = subprocess.check_output(
            ["aws", "athena", "get-query-execution", "--region", region, "--query-execution-id", qid],
            text=True,
        )
        st = json.loads(raw)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            return
        if st in ("FAILED", "CANCELLED"):
            reason = json.loads(raw)["QueryExecution"].get("Status", {}).get("StateChangeReason", "")
            raise RuntimeError(f"Athena {qid} {st}: {reason}\nSQL:\n{sql[:500]}")
        time.sleep(1)
    raise TimeoutError(qid)


def split_ddl(sql_text: str) -> list[str]:
    cleaned_lines: list[str] = []
    for line in sql_text.splitlines():
        t = line.strip()
        if not t or t.startswith("--"):
            continue
        cleaned_lines.append(line)
    text = "\n".join(cleaned_lines)
    chunks = re.split(r";\s*(?=CREATE\s)", text)
    return [c.strip() + ";" for c in chunks if c.strip()]


def main() -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "ap-south-1"))
    p.add_argument("--workgroup", default="primary")
    p.add_argument("--ddl-skip", action="store_true", help="Only sync run_id projection from S3")
    p.add_argument("--sync-skip", action="store_true", help="Only run DDL")
    args = p.parse_args()

    ddl_path = ROOT / "infra" / "aws" / "athena" / "ddl_gold_arbeitnow_slice.sql"
    if not ddl_path.exists():
        print(f"Missing {ddl_path}", file=sys.stderr)
        return 1

    stmts = split_ddl(ddl_path.read_text(encoding="utf-8"))
    if not args.ddl_skip:
        for i, stmt in enumerate(stmts):
            print(f"DDL {i + 1}/{len(stmts)}...", file=sys.stderr)
            run_sql(stmt, region=args.region, workgroup=args.workgroup, database=None)

    if not args.sync_skip:
        csv = sync_arbeitnow_slice_gold_projection_from_s3(region=args.region, workgroup=args.workgroup)
        print(f"Synced projection.run_id.values ({len(csv.split(','))} run_id(s))", file=sys.stderr)

    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
