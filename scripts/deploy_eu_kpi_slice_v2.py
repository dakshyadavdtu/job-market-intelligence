#!/usr/bin/env python3
"""Deploy jmi_analytics_v2 EU KPI slice + Silver foundation + role/employer + scatter + sankey (no DQ helpers)."""
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
    """Split multi-view SQL files. Handles ';' before CREATE and comment blocks between views."""
    text = sql_text.strip()
    if not text:
        return []
    parts = re.split(r"(?=CREATE\s+OR\s+REPLACE\s+VIEW\s)", text, flags=re.IGNORECASE | re.DOTALL)
    out: list[str] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if not re.match(r"^CREATE\s+OR\s+REPLACE\s+VIEW\s", p, re.IGNORECASE):
            continue
        out.append(p.rstrip(";").strip() + ";")
    return out if out else [text.rstrip(";").strip() + ";"]


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
    athena = ROOT / "infra" / "aws" / "athena"
    # Order: Silver foundation (base for sankey/skills/remote) → Gold role/employer → KPI/DQ/location/sankey
    deploy_file(athena / "analytics_v2_eu_silver_foundation.sql", "v2_eu_silver_jobs_base + skills_long")
    deploy_file(athena / "analytics_v2_eu_role_company_classified.sql", "v2_eu_role_titles + v2_eu_employers_top_clean")
    deploy_file(athena / "analytics_v2_eu_kpi_slice.sql", "v2_eu_kpi_slice_monthly")
    deploy_file(athena / "analytics_v2_eu_location_scatter.sql", "v2_eu_location_scatter_metrics")
    deploy_file(athena / "analytics_v2_eu_sankey_helper.sql", "v2_eu_sankey_location_to_company_monthly")
    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
