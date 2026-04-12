#!/usr/bin/env python3
"""
Create QuickSight datasets pointing at jmi_analytics_v2 views.
Requires: aws glue get-table, aws quicksight create-data-set, same region as Athena/Glue.
"""
from __future__ import annotations

import json
import subprocess
import sys
import uuid
from pathlib import Path

ACCOUNT = "470441577506"
REGION = "ap-south-1"
DATABASE = "jmi_analytics_v2"
# Existing Athena data source (same workgroup as v2)
DATA_SOURCE_ARN = (
    "arn:aws:quicksight:ap-south-1:470441577506:datasource/"
    "5fe598b5-11bf-482b-91ec-cd4be52f4eb4"
)

# (QuickSight display name, Glue view name)
V2_DATASETS: list[tuple[str, str]] = [
    # Europe / Arbeitnow (distinct prefix)
    ("JMI v2 EU — sheet1_kpis", "sheet1_kpis"),
    ("JMI v2 EU — skill_demand_monthly_latest", "skill_demand_monthly_latest"),
    ("JMI v2 EU — location_top15_other", "location_top15_other"),
    ("JMI v2 EU — role_pareto", "role_pareto"),
    ("JMI v2 EU — role_top20", "role_top20"),
    ("JMI v2 EU — company_top12_other", "company_top12_other"),
    ("JMI v2 EU — pipeline_run_summary_latest", "pipeline_run_summary_latest"),
    # India / Adzuna (distinct prefix)
    ("JMI v2 IN — skill_demand_monthly_adzuna_latest", "skill_demand_monthly_adzuna_latest"),
    ("JMI v2 IN — pipeline_run_summary_adzuna_latest", "pipeline_run_summary_adzuna_latest"),
    ("JMI v2 IN — location_top15_other_adzuna", "location_top15_other_adzuna"),
    ("JMI v2 IN — role_group_pareto_adzuna", "role_group_pareto_adzuna"),
    ("JMI v2 IN — role_group_top20_adzuna", "role_group_top20_adzuna"),
    ("JMI v2 IN — company_top15_other_clean_adzuna", "company_top15_other_clean_adzuna"),
    ("JMI v2 IN — role_group_demand_monthly_adzuna", "role_group_demand_monthly_adzuna"),
    ("JMI v2 IN — role_title_classified_adzuna", "role_title_classified_adzuna"),
]


def glue_to_qs_type(glue_type: str) -> str:
    t = glue_type.lower().strip()
    if t in ("string", "varchar", "char"):
        return "STRING"
    if t in ("boolean",):
        return "BIT"
    if "timestamp" in t or t == "date":
        return "DATETIME"
    if t in ("bigint", "int", "integer", "smallint", "tinyint"):
        return "INTEGER"
    if t in ("double", "float", "decimal", "real"):
        return "DECIMAL"
    if t.startswith("array") or t.startswith("map") or t.startswith("struct"):
        return "STRING"
    return "STRING"


def get_glue_columns(view: str) -> list[dict]:
    raw = subprocess.check_output(
        [
            "aws",
            "glue",
            "get-table",
            "--database-name",
            DATABASE,
            "--name",
            view,
            "--region",
            REGION,
        ],
        text=True,
    )
    table = json.loads(raw)["Table"]
    cols = table["StorageDescriptor"]["Columns"]
    out = []
    for c in cols:
        name = c["Name"]
        qs_type = glue_to_qs_type(c["Type"])
        # Omit Id — QuickSight generates IDs (explicit Id can trigger legacy-mode errors).
        out.append({"Name": name, "Type": qs_type})
    return out


def create_dataset(display_name: str, view: str) -> dict:
    cols = get_glue_columns(view)
    pt_id = "pt"
    lt_id = "lt"
    payload = {
        "AwsAccountId": ACCOUNT,
        "DataSetId": str(uuid.uuid4()),
        "Name": display_name,
        "PhysicalTableMap": {
            pt_id: {
                "RelationalTable": {
                    "DataSourceArn": DATA_SOURCE_ARN,
                    "Catalog": "AwsDataCatalog",
                    "Schema": DATABASE,
                    "Name": view,
                    "InputColumns": cols,
                }
            }
        },
        "LogicalTableMap": {
            lt_id: {
                "Alias": view,
                "Source": {"PhysicalTableId": pt_id},
            }
        },
        "ImportMode": "DIRECT_QUERY",
    }
    path = f"/tmp/qs_create_{view}.json"
    Path(path).write_text(json.dumps(payload), encoding="utf-8")
    subprocess.check_call(
        [
            "aws",
            "quicksight",
            "create-data-set",
            "--cli-input-json",
            f"file://{path}",
            "--region",
            REGION,
        ]
    )
    out = subprocess.check_output(
        [
            "aws",
            "quicksight",
            "describe-data-set",
            "--aws-account-id",
            ACCOUNT,
            "--data-set-id",
            payload["DataSetId"],
            "--region",
            REGION,
        ],
        text=True,
    )
    return {
        "DataSetId": payload["DataSetId"],
        "Name": display_name,
        "View": view,
        "Arn": json.loads(out)["DataSet"]["Arn"],
    }


def main() -> int:
    results = []
    for display_name, view in V2_DATASETS:
        try:
            print(f"Creating {display_name} -> {view}...", file=sys.stderr)
            r = create_dataset(display_name, view)
            results.append(r)
            print(json.dumps(r), file=sys.stderr)
        except subprocess.CalledProcessError as e:
            print(f"FAIL {display_name}: {e}", file=sys.stderr)
            return 1
    Path("/tmp/quicksight_v2_datasets.json").write_text(
        json.dumps(results, indent=2), encoding="utf-8"
    )
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
