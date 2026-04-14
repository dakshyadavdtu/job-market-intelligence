#!/usr/bin/env python3
"""Run Athena smoke queries for jmi_gold_v2 / jmi_analytics_v2; print TSV results."""
from __future__ import annotations

import json
import subprocess
import sys
import time

REGION = "ap-south-1"
WORKGROUP = "primary"
OUTPUT = "s3://jmi-dakshyadav-job-market-intelligence/athena-results/"

QUERIES: list[tuple[str, str | None, str]] = [
    ("meta_eu", "jmi_gold_v2", "SELECT * FROM jmi_gold_v2.latest_run_metadata_arbeitnow"),
    ("meta_ad", "jmi_gold_v2", "SELECT * FROM jmi_gold_v2.latest_run_metadata_adzuna"),
    (
        "cnt_skill_an",
        "jmi_gold_v2",
        "SELECT COUNT(*) AS n FROM jmi_gold_v2.skill_demand_monthly WHERE source = 'arbeitnow' AND posted_month BETWEEN '2018-01' AND '2035-12'",
    ),
    (
        "cnt_skill_ad",
        "jmi_gold_v2",
        "SELECT COUNT(*) AS n FROM jmi_gold_v2.skill_demand_monthly WHERE source = 'adzuna_in' AND posted_month BETWEEN '2018-01' AND '2035-12'",
    ),
    (
        "cnt_skill_an_run",
        "jmi_gold_v2",
        "SELECT COUNT(*) AS n FROM jmi_gold_v2.skill_demand_monthly WHERE source = 'arbeitnow' AND run_id = '20260411T170924Z-f61e46e1' AND posted_month BETWEEN '2018-01' AND '2035-12'",
    ),
    (
        "cnt_skill_ad_run",
        "jmi_gold_v2",
        "SELECT COUNT(*) AS n FROM jmi_gold_v2.skill_demand_monthly WHERE source = 'adzuna_in' AND run_id = '20260412T104501Z-2225d40a' AND posted_month BETWEEN '2018-01' AND '2035-12'",
    ),
    (
        "vw_v2_eu_roles",
        "jmi_analytics_v2",
        "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_eu_role_titles_classified",
    ),
    (
        "vw_v2_eu_employers",
        "jmi_analytics_v2",
        "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_eu_employers_top_clean",
    ),
    (
        "vw_v2_in_roles",
        "jmi_analytics_v2",
        "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_role_titles_classified",
    ),
    (
        "vw_v2_in_employers",
        "jmi_analytics_v2",
        "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_employers_top_clean",
    ),
    (
        "vw_cmp_skill_mix_aligned_top20",
        "jmi_analytics_v2",
        "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_skill_mix_aligned_top20",
    ),
    (
        "cmp_time_policy",
        "jmi_analytics_v2",
        "SELECT strict_intersection_latest_month, march_strict_comparable_both_sources, ten_year_window_claim_valid FROM jmi_analytics_v2.comparison_time_window_policy",
    ),
    (
        "cmp_strict_month_totals",
        "jmi_analytics_v2",
        "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_strict_intersection_month_totals",
    ),
    (
        "cmp_march_status",
        "jmi_analytics_v2",
        "SELECT source, has_march_posted_month_in_latest_run FROM jmi_analytics_v2.comparison_march_strict_status",
    ),
]


def run(q: str, database: str | None) -> dict:
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
        q,
    ]
    if database:
        cmd.extend(["--query-execution-context", f"Database={database}"])
    out = subprocess.check_output(cmd, text=True)
    qid = json.loads(out)["QueryExecutionId"]
    for _ in range(90):
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
            return json.loads(
                subprocess.check_output(
                    [
                        "aws",
                        "athena",
                        "get-query-results",
                        "--region",
                        REGION,
                        "--query-execution-id",
                        qid,
                    ],
                    text=True,
                )
            )
        if st in ("FAILED", "CANCELLED"):
            reason = json.loads(raw)["QueryExecution"].get("Status", {}).get(
                "StateChangeReason", ""
            )
            raise RuntimeError(f"{qid} {st}: {reason}")
        time.sleep(0.5)
    raise TimeoutError(qid)


def main() -> int:
    for name, db, sql in QUERIES:
        try:
            res = run(sql, db)
            rows = res.get("ResultSet", {}).get("Rows", [])
            print(f"OK\t{name}\t{rows}")
        except Exception as e:
            print(f"FAIL\t{name}\t{e}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
