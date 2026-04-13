#!/usr/bin/env python3
"""
Permanently remove presentation layer from Glue/Athena: DROP v2_presentation_* views,
then DROP presentation_* external tables in jmi_gold_v2.

Does not delete S3 objects under gold_v2/presentation/ (orphan legacy; see
infra/aws/athena/legacy_gold_v2_presentation_paths.txt).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

BUCKET = "jmi-dakshyadav-job-market-intelligence"


def athena_bucket() -> str:
    return os.environ.get("JMI_BUCKET", BUCKET).strip() or BUCKET


def athena_output_location() -> str:
    return f"s3://{athena_bucket()}/athena-results/"


def run_athena_sql(sql: str, *, region: str, workgroup: str, database: str | None) -> str:
    cmd = [
        "aws",
        "athena",
        "start-query-execution",
        "--region",
        region,
        "--work-group",
        workgroup,
        "--result-configuration",
        f"OutputLocation={athena_output_location()}",
        "--query-string",
        sql,
    ]
    if database:
        cmd.extend(["--query-execution-context", f"Database={database}"])
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)["QueryExecutionId"]


def wait_done(qid: str, region: str) -> None:
    for _ in range(120):
        out = subprocess.check_output(
            [
                "aws",
                "athena",
                "get-query-execution",
                "--region",
                region,
                "--query-execution-id",
                qid,
            ],
            text=True,
        )
        st = json.loads(out)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            return
        if st in ("FAILED", "CANCELLED"):
            reason = json.loads(out)["QueryExecution"].get("Status", {}).get("StateChangeReason", "")
            raise RuntimeError(f"Query {qid} {st}: {reason}")
        time.sleep(1)
    raise TimeoutError(qid)


def split_sql_statements(sql: str) -> list[str]:
    lines = sql.splitlines()
    blocks: list[str] = []
    cur: list[str] = []
    for line in lines:
        if re.match(r"^\s*DROP\s+(VIEW|TABLE)\b", line) and cur:
            blocks.append("\n".join(cur).strip())
            cur = [line]
        else:
            cur.append(line)
    if cur:
        blocks.append("\n".join(cur).strip())
    return [b for b in blocks if b and not b.strip().startswith("--")]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--region", default="ap-south-1")
    p.add_argument("--workgroup", default="primary")
    p.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    sql_path = args.repo_root / "infra" / "aws" / "athena" / "drop_presentation_layer.sql"
    raw = sql_path.read_text(encoding="utf-8")
    steps: list[tuple[str, str | None]] = []
    for stmt in split_sql_statements(raw):
        u = stmt.strip().upper()
        if u.startswith("DROP VIEW"):
            steps.append((stmt, "jmi_analytics_v2"))
        else:
            steps.append((stmt, "jmi_gold_v2"))

    print(f"Statements: {len(steps)}", file=sys.stderr)
    if args.dry_run:
        for i, (sql, db) in enumerate(steps):
            print(f"--- {i+1} db={db} ---\n{sql}\n")
        return 0

    for i, (sql, db) in enumerate(steps):
        print(f"Running {i+1}/{len(steps)} db={db}...", file=sys.stderr)
        qid = run_athena_sql(sql, region=args.region, workgroup=args.workgroup, database=db)
        wait_done(qid, args.region)
        print(f"  OK {qid}", file=sys.stderr)
    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
