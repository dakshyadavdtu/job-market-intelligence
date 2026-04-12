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
    ("meta_eu", "jmi_gold_v2", "SELECT * FROM jmi_gold_v2.latest_run_metadata"),
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
    ("vw_lr", "jmi_analytics_v2", "SELECT * FROM jmi_analytics_v2.latest_pipeline_run"),
    ("vw_lr_ad", "jmi_analytics_v2", "SELECT * FROM jmi_analytics_v2.latest_pipeline_run_adzuna"),
    ("vw_skill_latest", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.skill_demand_monthly_latest"),
    ("vw_skill_ad_latest", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.skill_demand_monthly_adzuna_latest"),
    ("vw_sheet1", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.sheet1_kpis"),
    ("vw_loc15", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.location_top15_other"),
    ("vw_loc15_ad", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.location_top15_other_adzuna"),
    ("vw_pareto_ad", "jmi_analytics_v2", "SELECT MAX(cumulative_job_pct) AS max_pct FROM jmi_analytics_v2.role_group_pareto_adzuna"),
    (
        "vw_cmp_src_month",
        "jmi_analytics_v2",
        "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_month_totals",
    ),
    (
        "vw_cmp_src_mix",
        "jmi_analytics_v2",
        "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_skill_mix",
    ),
    (
        "vw_cmp_src_hhi",
        "jmi_analytics_v2",
        "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_month_skill_tag_hhi",
    ),
    (
        "vw_cmp_src_top20",
        "jmi_analytics_v2",
        "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_skill_mix_aligned_top20",
    ),
    (
        "vw_cmp_benchmark",
        "jmi_analytics_v2",
        "SELECT * FROM jmi_analytics_v2.comparison_benchmark_aligned_month",
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
