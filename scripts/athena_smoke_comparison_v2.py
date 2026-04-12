#!/usr/bin/env python3
"""Smoke tests for jmi_analytics_v2 comparison helper views only."""
from __future__ import annotations

import json
import subprocess
import sys
import time

REGION = "ap-south-1"
WORKGROUP = "primary"
OUTPUT = "s3://jmi-dakshyadav-job-market-intelligence/athena-results/"
DB = "jmi_analytics_v2"

QUERIES: list[tuple[str, str]] = [
    ("cmp_src_month_ct", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_month_totals"),
    ("cmp_src_mix_ct", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_skill_mix"),
    ("cmp_src_hhi_ct", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_month_skill_tag_hhi"),
    ("cmp_src_top20_ct", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_skill_mix_aligned_top20"),
    (
        "cmp_src_month_sample",
        "SELECT source, ingest_month, total_postings FROM jmi_analytics_v2.comparison_source_month_totals ORDER BY source, ingest_month LIMIT 6",
    ),
    (
        "cmp_benchmark",
        "SELECT * FROM jmi_analytics_v2.comparison_benchmark_aligned_month",
    ),
    (
        "cmp_hhi_sample",
        "SELECT source, ingest_month, skill_tag_hhi FROM jmi_analytics_v2.comparison_source_month_skill_tag_hhi ORDER BY source, ingest_month LIMIT 4",
    ),
]


def run(q: str) -> dict:
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
        "--query-execution-context",
        f"Database={DB}",
        "--query-string",
        q,
    ]
    out = subprocess.check_output(cmd, text=True)
    qid = json.loads(out)["QueryExecutionId"]
    for _ in range(120):
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
            reason = json.loads(raw)["QueryExecution"].get("Status", {}).get("StateChangeReason", "")
            raise RuntimeError(f"{qid} {st}: {reason}")
        time.sleep(0.5)
    raise TimeoutError(qid)


def main() -> int:
    for name, sql in QUERIES:
        try:
            res = run(sql)
            rows = res.get("ResultSet", {}).get("Rows", [])
            print(f"OK\t{name}\t{rows}")
        except Exception as e:
            print(f"FAIL\t{name}\t{e}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
