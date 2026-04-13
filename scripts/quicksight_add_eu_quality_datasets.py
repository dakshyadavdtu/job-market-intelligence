#!/usr/bin/env python3
"""Create EU role_group + company_top15_other_clean datasets (after Athena quality views)."""
from __future__ import annotations

import json
import subprocess
import sys
import uuid
from pathlib import Path

# Copy-paste from quicksight_create_datasets_v2 (same glue_to_qs + create_dataset)
import re

ACCOUNT = "470441577506"
REGION = "ap-south-1"
DATABASE = "jmi_analytics_v2"
DATA_SOURCE_ARN = (
    "arn:aws:quicksight:ap-south-1:470441577506:datasource/"
    "5fe598b5-11bf-482b-91ec-cd4be52f4eb4"
)

EXTRA = [
    ("JMI v2 EU — role_group_pareto", "role_group_pareto"),
    ("JMI v2 EU — company_top15_other_clean", "company_top15_other_clean"),
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
    return [{"Name": c["Name"], "Type": glue_to_qs_type(c["Type"])} for c in cols]


def create_dataset(display_name: str, view: str) -> dict:
    cols = get_glue_columns(view)
    payload = {
        "AwsAccountId": ACCOUNT,
        "DataSetId": str(uuid.uuid4()),
        "Name": display_name,
        "PhysicalTableMap": {
            "pt": {
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
            "lt": {
                "Alias": view,
                "Source": {"PhysicalTableId": "pt"},
            }
        },
        "ImportMode": "DIRECT_QUERY",
    }
    p = Path(f"/tmp/qs_{view}.json")
    p.write_text(json.dumps(payload), encoding="utf-8")
    subprocess.check_call(
        [
            "aws",
            "quicksight",
            "create-data-set",
            "--cli-input-json",
            f"file://{p}",
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
    out = []
    for name, view in EXTRA:
        print(f"Creating {name}...", file=sys.stderr)
        out.append(create_dataset(name, view))
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
