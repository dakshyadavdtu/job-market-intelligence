#!/usr/bin/env python3
"""Run Athena smoke queries for jmi_gold_v2 / jmi_analytics_v2 (dea final 6 retained views)."""
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
    ("vw_v2_eu_kpi", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_eu_kpi_slice_monthly"),
    ("vw_v2_in_kpi", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_kpi_slice_monthly"),
    ("vw_v2_eu_roles", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_eu_role_titles_classified"),
    ("vw_v2_eu_employers", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_eu_employers_top_clean"),
    ("vw_v2_in_roles", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_role_titles_classified"),
    ("vw_v2_in_employers", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_employers_top_clean"),
    ("vw_in_heatmap", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_heatmap_state_skill_monthly"),
    ("vw_in_radar", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_radar_profile_monthly"),
    ("vw_in_sankey", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_sankey_state_to_company_monthly"),
    ("vw_in_geo_state", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_geo_state_monthly"),
    ("vw_in_skills_long", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_in_silver_jobs_skills_long"),
    ("vw_eu_scatter", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_eu_location_scatter_metrics"),
    ("vw_eu_sankey", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_eu_sankey_location_to_company_monthly"),
    ("vw_eu_skills_long", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_eu_silver_jobs_skills_long"),
    ("vw_cmp_skill_mix_aligned_top20", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_skill_mix_aligned_top20"),
    ("vw_cmp_benchmark", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_benchmark_aligned_month"),
    ("vw_cmp_hhi_helper", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.comparison_source_month_skill_tag_hhi"),
    ("vw_cmp_spj", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_cmp_skills_per_job_april_2026"),
    ("vw_cmp_loc_hhi", "jmi_analytics_v2", "SELECT COUNT(*) AS n FROM jmi_analytics_v2.v2_cmp_location_hhi_monthly"),
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
    for _ in range(180):
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
        time.sleep(1.0)
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
