#!/usr/bin/env python3
"""
Deploy jmi_gold_v2 + jmi_analytics_v2 to Athena (Glue catalog).
Requires: aws CLI credentials, S3 output for Athena, workgroup.

Comparison helpers (EU vs IN benchmark views): run
`scripts/deploy_athena_comparison_views_v2.py` after this deploy.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

BUCKET = "jmi-dakshyadav-job-market-intelligence"
OUTPUT = f"s3://{BUCKET}/athena-results/"
# Live uploaded v2 run_ids (must appear in projection.run_id.values)
RUN_ID_ENUM = ",".join(
    [
        "20260411T170924Z-f61e46e1",
        "20260412T024632Z-a951261b",
        "20260412T064632Z-2d7a6775",
        "20260412T102534Z-ca1b73ff",
        "20260412T104501Z-2225d40a",
        "20260412T162800Z-533e581f",
    ]
)


def run_athena_sql(
    sql: str,
    *,
    region: str,
    workgroup: str,
    database: str | None,
) -> str:
    cmd = [
        "aws",
        "athena",
        "start-query-execution",
        "--region",
        region,
        "--work-group",
        workgroup,
        "--result-configuration",
        f"OutputLocation={OUTPUT}",
        "--query-string",
        sql,
    ]
    if database:
        cmd.extend(["--query-execution-context", f"Database={database}"])
    out = subprocess.check_output(cmd, text=True)
    import json

    qid = json.loads(out)["QueryExecutionId"]
    return qid


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
        import json

        st = json.loads(out)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            return
        if st in ("FAILED", "CANCELLED"):
            reason = json.loads(out)["QueryExecution"].get("Status", {}).get("StateChangeReason", "")
            raise RuntimeError(f"Query {qid} {st}: {reason}")
        time.sleep(1)
    raise TimeoutError(qid)


def patch_ddl(content: str) -> str:
    c = content.replace("jmi_gold.", "jmi_gold_v2.")
    c = re.sub(
        r"'projection\.run_id\.values'\s*=\s*'[^']*'",
        f"'projection.run_id.values' = '{RUN_ID_ENUM}'",
        c,
    )
    return c


def strip_line_comments(sql: str) -> str:
    out: list[str] = []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        out.append(line)
    return "\n".join(out).strip()


def split_sql_statements(sql: str) -> list[str]:
    """Split on CREATE ... at line start; strip -- lines (Athena allows one statement per API call)."""
    lines = sql.splitlines()
    blocks: list[str] = []
    cur: list[str] = []
    for line in lines:
        if (
            re.match(r"^\s*CREATE\s+(OR\s+REPLACE\s+)?(VIEW|DATABASE)\b", line)
            and cur
        ):
            blocks.append(strip_line_comments("\n".join(cur)))
            cur = [line]
        else:
            cur.append(line)
    if cur:
        blocks.append(strip_line_comments("\n".join(cur)))
    return [b for b in blocks if b and not b.startswith("--")]


def main() -> int:
    p = argparse.ArgumentParser()
    # Must match S3 bucket region (Athena query results + Glue catalog).
    p.add_argument("--region", default="ap-south-1")
    p.add_argument("--workgroup", default="primary")
    p.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    infra = args.repo_root / "infra" / "aws" / "athena"
    docs = args.repo_root / "docs" / "dashboard_implementation"

    ddl_files = [
        infra / "ddl_gold_latest_run_metadata.sql",
        infra / "ddl_gold_latest_run_metadata_adzuna.sql",
        infra / "ddl_gold_skill_demand_monthly.sql",
        infra / "ddl_gold_role_demand_monthly.sql",
        infra / "ddl_gold_location_demand_monthly.sql",
        infra / "ddl_gold_company_hiring_monthly.sql",
        infra / "ddl_gold_pipeline_run_summary.sql",
    ]

    steps: list[tuple[str, str | None]] = []
    steps.append(("CREATE DATABASE IF NOT EXISTS jmi_gold_v2;", None))

    for f in ddl_files:
        raw = f.read_text(encoding="utf-8")
        patched = patch_ddl(raw)
        # strip leading comments-only lines for cleaner execution
        lines = []
        for line in patched.splitlines():
            if lines or line.strip().startswith("CREATE"):
                lines.append(line)
        sql = "\n".join(lines).strip()
        steps.append((sql, "jmi_gold_v2"))

    views_eu = (docs / "ATHENA_VIEWS.sql").read_text(encoding="utf-8")
    views_eu = views_eu.replace("jmi_gold.", "jmi_gold_v2.")
    views_eu = views_eu.replace("jmi_analytics.", "jmi_analytics_v2.")
    views_ad = (docs / "ATHENA_VIEWS_ADZUNA.sql").read_text(encoding="utf-8")
    views_ad = views_ad.replace("jmi_gold.", "jmi_gold_v2.")
    views_ad = views_ad.replace("jmi_analytics.", "jmi_analytics_v2.")

    steps.append(("CREATE DATABASE IF NOT EXISTS jmi_analytics_v2;", None))
    for stmt in split_sql_statements(views_eu):
        if "CREATE DATABASE" in stmt:
            continue
        steps.append((stmt, "jmi_analytics_v2"))
    for stmt in split_sql_statements(views_ad):
        if "CREATE DATABASE" in stmt:
            continue
        steps.append((stmt, "jmi_analytics_v2"))

    print(f"Total statements: {len(steps)}", file=sys.stderr)
    if args.dry_run:
        for i, (sql, db) in enumerate(steps):
            print(f"--- {i+1} db={db} ---\n{sql[:200]}...\n")
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
