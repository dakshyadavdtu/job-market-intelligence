#!/usr/bin/env python3
"""
Deploy jmi_gold_v2 + jmi_analytics_v2 to Athena (Glue catalog).
Requires: aws CLI credentials, S3 output for Athena, workgroup.

jmi_analytics_v2 is deployed only via `scripts/deploy_jmi_analytics_v2_minimal.py`
(five v2_* views). Do not point ATHENA_VIEWS.sql at jmi_analytics_v2 here — that
would recreate legacy convenience views and old names.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path

BUCKET = "jmi-dakshyadav-job-market-intelligence"


def athena_bucket() -> str:
    """Bucket for Athena query results and (by convention) JMI data sync."""
    return os.environ.get("JMI_BUCKET", BUCKET).strip() or BUCKET


def athena_output_location() -> str:
    return f"s3://{athena_bucket()}/athena-results/"


# Back-compat alias used by older callers
OUTPUT = athena_output_location()
# Live uploaded v2 run_ids (must appear in projection.run_id.values)
# Keep in sync with S3 gold/*/source=*/posted_month=*/run_id=*/ and infra/aws/athena/ddl_gold_*.sql
RUN_ID_ENUM = ",".join(
    [
        "20260411T031647Z-dbb24c8f",
        "20260411T031717Z-d78c6b49",
        "20260411T124053Z-a0c1f1e6",
        "20260411T134219Z-7ee39ce2",
        "20260411T170924Z-f61e46e1",
        "20260412T020715Z-fd9cca4e",
        "20260412T024632Z-a951261b",
        "20260412T064632Z-2d7a6775",
        "20260412T102534Z-ca1b73ff",
        "20260412T104501Z-2225d40a",
        "20260412T104632Z-921e5efe",
        "20260412T144631Z-06c081f1",
        "20260412T155712Z-e2e07b3f",
        "20260412T162800Z-533e581f",
        "20260412T170441Z-647d45a3",
        "20260412T170721Z-2366d12e",
        "20260412T171215Z-afe6422c",
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
        f"OutputLocation={athena_output_location()}",
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


# Gold v2 fact tables that use partition projection on run_id (see ddl_gold_*.sql).
GOLD_V2_RUN_PROJECTION_TABLES = (
    "skill_demand_monthly",
    "role_demand_monthly",
    "location_demand_monthly",
    "company_hiring_monthly",
    "pipeline_run_summary",
)


def update_gold_v2_run_id_projection(
    run_id_csv: str,
    *,
    region: str,
    workgroup: str,
) -> None:
    """Set Glue projection.run_id.values on all jmi_gold_v2 fact tables (comma-separated run_ids)."""
    if "'" in run_id_csv:
        raise ValueError("run_id CSV must not contain single quotes")
    for name in GOLD_V2_RUN_PROJECTION_TABLES:
        sql = (
            f"ALTER TABLE jmi_gold_v2.{name} SET TBLPROPERTIES "
            f"('projection.run_id.values'='{run_id_csv}')"
        )
        qid = run_athena_sql(sql, region=region, workgroup=workgroup, database="jmi_gold_v2")
        wait_done(qid, region)


def patch_ddl(content: str) -> str:
    c = content.replace("jmi_gold.", "jmi_gold_v2.")
    c = re.sub(
        r"'projection\.run_id\.values'\s*=\s*'[^']*'",
        f"'projection.run_id.values' = '{RUN_ID_ENUM}'",
        c,
    )
    return c


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

    steps.append(("CREATE DATABASE IF NOT EXISTS jmi_analytics_v2;", None))
    minimal_analytics = args.repo_root / "scripts" / "deploy_jmi_analytics_v2_minimal.py"
    steps.append(("__RUN_MINIMAL_ANALYTICS__", str(minimal_analytics)))

    print(f"Total statements: {len(steps)}", file=sys.stderr)
    if args.dry_run:
        for i, (sql, db) in enumerate(steps):
            if sql == "__RUN_MINIMAL_ANALYTICS__":
                print(f"--- {i+1} --- subprocess: {db}\n")
            else:
                print(f"--- {i+1} db={db} ---\n{sql[:200]}...\n")
        return 0

    for i, (sql, db) in enumerate(steps):
        if sql == "__RUN_MINIMAL_ANALYTICS__":
            print(f"Running {i+1}/{len(steps)} jmi_analytics_v2 (minimal v2_* views)...", file=sys.stderr)
            subprocess.check_call([sys.executable, db], env=os.environ)
            print("  OK deploy_jmi_analytics_v2_minimal.py", file=sys.stderr)
            continue
        print(f"Running {i+1}/{len(steps)} db={db}...", file=sys.stderr)
        qid = run_athena_sql(sql, region=args.region, workgroup=args.workgroup, database=db)
        wait_done(qid, args.region)
        print(f"  OK {qid}", file=sys.stderr)

    # Drop views removed from repo SQL (thin wrappers / duplicates / demos) so Glue catalog matches minimum jmi_analytics_v2.
    obsolete_analytics_v2 = [
        "DROP VIEW IF EXISTS jmi_analytics_v2.latest_pipeline_run",
        "DROP VIEW IF EXISTS jmi_analytics_v2.skill_demand_monthly_latest",
        "DROP VIEW IF EXISTS jmi_analytics_v2.pipeline_run_summary_latest",
        "DROP VIEW IF EXISTS jmi_analytics_v2.company_top12_other",
        "DROP VIEW IF EXISTS jmi_analytics_v2.role_top20",
        "DROP VIEW IF EXISTS jmi_analytics_v2.latest_pipeline_run_adzuna",
        "DROP VIEW IF EXISTS jmi_analytics_v2.skill_demand_monthly_adzuna_latest",
        "DROP VIEW IF EXISTS jmi_analytics_v2.pipeline_run_summary_adzuna_latest",
        "DROP VIEW IF EXISTS jmi_analytics_v2.role_group_top20_adzuna",
        "DROP VIEW IF EXISTS jmi_analytics_v2.sheet1_kpis_adzuna_latest",
        "DROP VIEW IF EXISTS jmi_analytics_v2.in_demo_sankey_location_role_proxy",
        "DROP VIEW IF EXISTS jmi_analytics_v2.in_demo_funnel_stages",
        "DROP VIEW IF EXISTS jmi_analytics_v2.in_demo_radar_profile_adzuna",
        "DROP VIEW IF EXISTS jmi_analytics_v2.in_demo_location_map_points",
        "DROP VIEW IF EXISTS jmi_analytics_v2.in_demo_skill_month_heatmap",
        "DROP VIEW IF EXISTS jmi_analytics_v2.role_group_top20",
    ]
    for j, stmt in enumerate(obsolete_analytics_v2):
        print(f"Running obsolete drop {j+1}/{len(obsolete_analytics_v2)}...", file=sys.stderr)
        qid = run_athena_sql(stmt, region=args.region, workgroup=args.workgroup, database="jmi_analytics_v2")
        wait_done(qid, args.region)
        print(f"  OK {qid}", file=sys.stderr)

    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
