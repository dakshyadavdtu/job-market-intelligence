#!/usr/bin/env python3
"""Create QuickSight datasets for jmi_analytics_v2 comparison helper views (idempotent by name skip)."""
from __future__ import annotations

import json
import subprocess
import sys
import uuid
from pathlib import Path

ACCOUNT = "470441577506"
REGION = "ap-south-1"
DATABASE = "jmi_analytics_v2"
# jmi-athena — required for create-data-set in this account (jmi_analytics DS may reject).
DATA_SOURCE_ARN = (
    "arn:aws:quicksight:ap-south-1:470441577506:datasource/"
    "6ce05f07-8388-4694-a2bc-af4924dc4700"
)

# Display name -> Glue view name (must match ATHENA_VIEWS_COMPARISON_V2.sql)
DATASETS: list[tuple[str, str]] = [
    ("JMI v2 — comparison_source_month_totals", "comparison_source_month_totals"),
    (
        "JMI v2 — comparison_source_skill_mix_aligned_top20",
        "comparison_source_skill_mix_aligned_top20",
    ),
    (
        "JMI v2 — comparison_benchmark_aligned_month",
        "comparison_benchmark_aligned_month",
    ),
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


def existing_dataset_arn_by_name(name: str) -> str | None:
    out = subprocess.check_output(
        [
            "aws",
            "quicksight",
            "list-data-sets",
            "--aws-account-id",
            ACCOUNT,
            "--region",
            REGION,
        ],
        text=True,
    )
    for s in json.loads(out).get("DataSetSummaries", []):
        if s.get("Name") == name:
            return s["Arn"]
    return None


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
    path = f"/tmp/qs_cmp_{view}.json"
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
    for display_name, view in DATASETS:
        ex = existing_dataset_arn_by_name(display_name)
        if ex:
            print(f"SKIP exists: {display_name}", file=sys.stderr)
            results.append({"Name": display_name, "View": view, "Arn": ex, "skipped": True})
            continue
        print(f"Creating {display_name}...", file=sys.stderr)
        r = create_dataset(display_name, view)
        results.append(r)
        print(json.dumps(r), file=sys.stderr)
    Path("/tmp/quicksight_comparison_v2_datasets.json").write_text(
        json.dumps(results, indent=2), encoding="utf-8"
    )
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
