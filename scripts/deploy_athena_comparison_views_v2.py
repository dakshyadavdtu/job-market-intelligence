#!/usr/bin/env python3
"""Deploy docs/dashboard_implementation/ATHENA_VIEWS_COMPARISON_V2.sql to Athena (jmi_analytics_v2)."""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

BUCKET = "jmi-dakshyadav-job-market-intelligence"
OUTPUT = f"s3://{BUCKET}/athena-results/"


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
        f"OutputLocation={OUTPUT}",
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


def strip_line_comments(sql: str) -> str:
    out: list[str] = []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        out.append(line)
    return "\n".join(out).strip()


def split_sql_statements(sql: str) -> list[str]:
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
    p.add_argument("--region", default="ap-south-1")
    p.add_argument("--workgroup", default="primary")
    p.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    sql_path = args.repo_root / "docs" / "dashboard_implementation" / "ATHENA_VIEWS_COMPARISON_V2.sql"
    raw = sql_path.read_text(encoding="utf-8")
    # Drop views removed from SQL so Glue matches repo (order: dependents before bases where needed).
    drop_pruned: list[str] = [
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_source_month_skill_tag_hhi",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_strict_common_benchmark_summary",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_strict_common_month_totals",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_strict_intersection_month_totals",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_source_month_totals",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_time_window_policy",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_strict_intersection_months",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_in_geo_city_points_monthly",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_in_silver_remote_classified_monthly",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_in_gold_skill_rows_monthly",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_eu_silver_remote_classified_monthly",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_eu_gold_skill_rows_monthly",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_in_silver_data_coverage_funnel_monthly",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_yearly_exploratory_manifest",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_yearly_exploratory_source_year_totals",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_exploratory_calendar_year_asymmetry_panel",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_exploratory_calendar_year_totals",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_observed_time_span",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_march_strict_role_mix",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_march_strict_skill_mix",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_march_strict_manifest",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_march_strict_benchmark_summary",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_march_strict_month_totals",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_strict_common_role_mix",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_strict_common_skill_mix",
        "DROP VIEW IF EXISTS jmi_analytics_v2.v2_strict_common_manifest",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_strict_intersection_role_demand",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_strict_intersection_skill_demand",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_march_strict_status",
    ]
    steps: list[tuple[str, str | None]] = []
    for stmt in split_sql_statements(raw):
        if "CREATE DATABASE" in stmt:
            steps.append((stmt, None))
        else:
            steps.append((stmt, "jmi_analytics_v2"))

    print(f"Statements: {len(steps)}", file=sys.stderr)
    if args.dry_run:
        for i, stmt in enumerate(drop_pruned):
            print(f"--- drop {i+1} ---\n{stmt}\n")
        for i, (sql, db) in enumerate(steps):
            print(f"--- {i+1} db={db} ---\n{sql[:300]}...\n")
        return 0

    for i, stmt in enumerate(drop_pruned):
        print(f"Running drop_pruned {i+1}/{len(drop_pruned)}...", file=sys.stderr)
        qid = run_athena_sql(stmt, region=args.region, workgroup=args.workgroup, database="jmi_analytics_v2")
        wait_done(qid, args.region)
        print(f"  OK {qid}", file=sys.stderr)

    for i, (sql, db) in enumerate(steps):
        print(f"Running {i+1}/{len(steps)} db={db}...", file=sys.stderr)
        qid = run_athena_sql(sql, region=args.region, workgroup=args.workgroup, database=db)
        wait_done(qid, args.region)
        print(f"  OK {qid}", file=sys.stderr)
    obsolete_cmp = [
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_source_skill_mix",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_radar_profile_dual",
        "DROP VIEW IF EXISTS jmi_analytics_v2.comparison_waterfall_benchmark_proxy",
    ]
    for j, stmt in enumerate(obsolete_cmp):
        print(f"Running obsolete comparison drop {j+1}/{len(obsolete_cmp)}...", file=sys.stderr)
        qid = run_athena_sql(stmt, region=args.region, workgroup=args.workgroup, database="jmi_analytics_v2")
        wait_done(qid, args.region)
        print(f"  OK {qid}", file=sys.stderr)
    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
