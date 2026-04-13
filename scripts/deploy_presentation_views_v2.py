#!/usr/bin/env python3
"""Deploy jmi_analytics_v2 pass-through views over jmi_gold_v2.presentation_* tables (views only)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
BUCKET = os.environ.get("JMI_BUCKET", "jmi-dakshyadav-job-market-intelligence").strip() or "jmi-dakshyadav-job-market-intelligence"
OUTPUT = f"s3://{BUCKET}/athena-results/"


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


def main() -> int:
    stmts = [
        "CREATE OR REPLACE VIEW jmi_analytics_v2.v2_presentation_skill_demand_monthly AS SELECT * FROM jmi_gold_v2.presentation_skill_demand_monthly",
        "CREATE OR REPLACE VIEW jmi_analytics_v2.v2_presentation_skill_demand_yearly AS SELECT * FROM jmi_gold_v2.presentation_skill_demand_yearly",
        "CREATE OR REPLACE VIEW jmi_analytics_v2.v2_presentation_role_demand_monthly AS SELECT * FROM jmi_gold_v2.presentation_role_demand_monthly",
        "CREATE OR REPLACE VIEW jmi_analytics_v2.v2_presentation_role_demand_yearly AS SELECT * FROM jmi_gold_v2.presentation_role_demand_yearly",
        "CREATE OR REPLACE VIEW jmi_analytics_v2.v2_presentation_location_demand_monthly AS SELECT * FROM jmi_gold_v2.presentation_location_demand_monthly",
        "CREATE OR REPLACE VIEW jmi_analytics_v2.v2_presentation_location_demand_yearly AS SELECT * FROM jmi_gold_v2.presentation_location_demand_yearly",
        "CREATE OR REPLACE VIEW jmi_analytics_v2.v2_presentation_company_hiring_monthly AS SELECT * FROM jmi_gold_v2.presentation_company_hiring_monthly",
        "CREATE OR REPLACE VIEW jmi_analytics_v2.v2_presentation_company_hiring_yearly AS SELECT * FROM jmi_gold_v2.presentation_company_hiring_yearly",
        "CREATE OR REPLACE VIEW jmi_analytics_v2.v2_presentation_pipeline_run_summary_monthly AS SELECT * FROM jmi_gold_v2.presentation_pipeline_run_summary_monthly",
        "CREATE OR REPLACE VIEW jmi_analytics_v2.v2_presentation_pipeline_run_summary_yearly AS SELECT * FROM jmi_gold_v2.presentation_pipeline_run_summary_yearly",
    ]
    for i, stmt in enumerate(stmts, 1):
        qid = run_sql(stmt)
        wait(qid)
        print(f"OK {i}/{len(stmts)} {qid}", file=sys.stderr)
    print("ALL_OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
