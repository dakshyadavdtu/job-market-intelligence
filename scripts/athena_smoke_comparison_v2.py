#!/usr/bin/env python3
"""Smoke tests for jmi_analytics_v2 comparison helpers (canonical comparison_* views)."""
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
    ("cmp_mix_ct", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_skill_mix_aligned_top20"),
    (
        "cmp_mix_sample",
        "SELECT source, skill, posted_month, skill_tag_count FROM jmi_analytics_v2.comparison_source_skill_mix_aligned_top20 ORDER BY source, skill LIMIT 8",
    ),
    (
        "cmp_spj_april_group",
        "SELECT posted_month, source, COUNT(*) AS n FROM jmi_analytics_v2.v2_cmp_skills_per_job_april_2026 GROUP BY 1,2 ORDER BY 1,2",
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
