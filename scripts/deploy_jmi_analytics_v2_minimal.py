#!/usr/bin/env python3
"""Deploy only strict minimal jmi_analytics_v2 views (v2_* names), then drop legacy names."""
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
DOCS = ROOT / "docs" / "dashboard_implementation"


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


def between(text: str, start: str, end: str) -> str:
    if start not in text:
        raise ValueError(f"start marker not found: {start[:80]}")
    tail = text.split(start, 1)[1]
    if end not in tail:
        raise ValueError(f"end marker not found: {end[:80]}")
    return tail.split(end, 1)[0].strip()


def cut_at_phrase(body: str, phrase: str) -> str:
    i = body.find(phrase)
    if i == -1:
        raise ValueError(f"cut phrase not found: {phrase!r}")
    return body[: i + len(phrase)].strip()


def patch_gold_v2(body: str) -> str:
    return body.replace("jmi_gold.", "jmi_gold_v2.")


def main() -> int:
    # Canonical skill-mix comparison view is comparison_source_skill_mix_aligned_top20
    # (deployed by deploy_athena_comparison_views_v2.py). Drop legacy duplicate alias only.
    print("Dropping redundant alias v2_cmp_skill_mix_aligned_top20...", file=sys.stderr)
    qid_drop = run_sql("DROP VIEW IF EXISTS jmi_analytics_v2.v2_cmp_skill_mix_aligned_top20")
    wait(qid_drop)
    print(f"  OK {qid_drop}", file=sys.stderr)

    role_file = (DOCS / "ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql").read_text(encoding="utf-8")
    adzuna_file = (DOCS / "ATHENA_VIEWS_ADZUNA.sql").read_text(encoding="utf-8")

    eu_rc = (ROOT / "infra" / "aws" / "athena" / "analytics_v2_eu_role_company_classified.sql").read_text(
        encoding="utf-8"
    )
    eu_parts = re.split(r"(?=CREATE\s+OR\s+REPLACE\s+VIEW\s+jmi_analytics_v2\.v2_eu_)", eu_rc.strip())
    eu_stmts = [p.strip() for p in eu_parts if re.match(r"^CREATE\s+OR\s+REPLACE", p.strip(), re.I)]
    if len(eu_stmts) != 2:
        raise RuntimeError(
            f"expected 2 EU views in analytics_v2_eu_role_company_classified.sql, got {len(eu_stmts)}"
        )
    eu_role_stmt = eu_stmts[0].rstrip().rstrip(";") + ";"
    eu_co_stmt = eu_stmts[1].rstrip().rstrip(";") + ";"

    in_role = cut_at_phrase(
        between(
            adzuna_file,
            "CREATE OR REPLACE VIEW jmi_analytics.role_title_classified_adzuna AS\n",
            "CREATE OR REPLACE VIEW jmi_analytics.role_group_demand_monthly_adzuna AS",
        ),
        "FROM classified;",
    )
    in_co = cut_at_phrase(
        between(
            adzuna_file,
            "CREATE OR REPLACE VIEW jmi_analytics.company_top15_other_clean_adzuna AS\n",
            "-- -----------------------------------------------------------------------------\n-- sheet1_kpis_adzuna",
        ),
        "WHERE job_count > 0;",
    )

    statements: list[str] = [
        eu_role_stmt,
        eu_co_stmt,
        f"CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_role_titles_classified AS\n{patch_gold_v2(in_role)}",
        f"CREATE OR REPLACE VIEW jmi_analytics_v2.v2_in_employers_top_clean AS\n{patch_gold_v2(in_co)}",
    ]

    print("Creating 4 v2_* views...", file=sys.stderr)
    for i, stmt in enumerate(statements, 1):
        qid = run_sql(stmt)
        wait(qid)
        print(f"  OK {i}/4 {qid}", file=sys.stderr)

    legacy = [
        "company_top15_other_clean",
        "company_top15_other_clean_adzuna",
        "comparison_benchmark_aligned_month",
        "comparison_source_month_skill_tag_hhi",
        "comparison_source_month_totals",
        "comparison_source_skill_mix_aligned_top20",
        "location_top15_other",
        "location_top15_other_adzuna",
        "role_group_demand_monthly",
        "role_group_demand_monthly_adzuna",
        "role_group_pareto",
        "role_group_pareto_adzuna",
        "role_pareto",
        "role_title_classified",
        "role_title_classified_adzuna",
        "sheet1_kpis",
        "sheet1_kpis_adzuna",
    ]
    print("Dropping legacy view names...", file=sys.stderr)
    for name in legacy:
        qid = run_sql(f"DROP VIEW IF EXISTS jmi_analytics_v2.{name}")
        wait(qid)
        print(f"  dropped {name}", file=sys.stderr)

    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
