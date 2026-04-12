#!/usr/bin/env python3
"""
Restore the EU employer table visual to use the analysis calculated field `employer_sort_group`.

`employer_sort_group` is not a physical Athena column; it is defined under Definition.CalculatedFields
(ifelse on company_label). Automated validation that only compares visuals to describe_data_set
OutputColumns will false-positive on calculated fields.

This script restores GroupBy + RowSort so the long-tail row sorts after top employers as intended.
"""

from __future__ import annotations

import copy
import time
from typing import Any, Dict

import boto3

REGION = "ap-south-1"
ACCOUNT = "470441577506"
ANALYSIS_ID = "jmi-v2-analysis-production-eu"
DASHBOARD_ID = "jmi-v2-dashboard-production"

EMPLOYER_SORT_FID = "company_t-employer_-sort.0.1775309921244"
COMPANY_LABEL_FID = "company_t-company_l-cfe6d6.0.1775309921244"
JOB_COUNT_FID = "company_t-job_count-d39853.1.1775309923958"


def restore_company_table_visuals(defn: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(defn)
    for sheet in out.get("Sheets", []):
        if "market intelligence" not in sheet.get("Name", "").lower():
            continue
        for vwrap in sheet.get("Visuals", []):
            for _kind, inner in vwrap.items():
                if not isinstance(inner, dict):
                    continue
                wells = (
                    inner.get("ChartConfiguration", {})
                    .get("FieldWells", {})
                    .get("TableAggregatedFieldWells")
                )
                if not wells:
                    continue
                gb = wells.get("GroupBy") or []
                # Target: Employer concentration table on company_top15_other_clean
                if not any(
                    x.get("CategoricalDimensionField", {})
                    .get("Column", {})
                    .get("ColumnName")
                    == "company_label"
                    and x.get("CategoricalDimensionField", {})
                    .get("Column", {})
                    .get("DataSetIdentifier")
                    == "company_top15_other_clean"
                    for x in gb
                ):
                    continue
                wells["GroupBy"] = [
                    {
                        "NumericalDimensionField": {
                            "FieldId": EMPLOYER_SORT_FID,
                            "Column": {
                                "DataSetIdentifier": "company_top15_other_clean",
                                "ColumnName": "employer_sort_group",
                            },
                        }
                    },
                    {
                        "CategoricalDimensionField": {
                            "FieldId": COMPANY_LABEL_FID,
                            "Column": {
                                "DataSetIdentifier": "company_top15_other_clean",
                                "ColumnName": "company_label",
                            },
                        }
                    },
                ]
                inner.setdefault("ChartConfiguration", {})["SortConfiguration"] = {
                    "RowSort": [
                        {
                            "FieldSort": {
                                "FieldId": EMPLOYER_SORT_FID,
                                "Direction": "ASC",
                            }
                        },
                        {
                            "FieldSort": {
                                "FieldId": JOB_COUNT_FID,
                                "Direction": "DESC",
                            }
                        },
                    ]
                }
    return out


def main() -> None:
    client = boto3.client("quicksight", region_name=REGION)

    a = client.describe_analysis_definition(AwsAccountId=ACCOUNT, AnalysisId=ANALYSIS_ID)
    fixed_a = restore_company_table_visuals(a["Definition"])
    client.update_analysis(
        AwsAccountId=ACCOUNT,
        AnalysisId=ANALYSIS_ID,
        Name=a["Name"],
        Definition=fixed_a,
    )
    print(f"Updated analysis {ANALYSIS_ID} (restored EU company table + calculated field sort)")

    d = client.describe_dashboard_definition(AwsAccountId=ACCOUNT, DashboardId=DASHBOARD_ID)
    fixed_d = restore_company_table_visuals(d["Definition"])
    client.update_dashboard(
        AwsAccountId=ACCOUNT,
        DashboardId=DASHBOARD_ID,
        Name=d["Name"],
        Definition=fixed_d,
        VersionDescription="Restore EU employer table calculated-field sort",
    )
    print(f"Updated dashboard draft {DASHBOARD_ID}")

    deadline = time.time() + 120
    latest = 0
    while time.time() < deadline:
        vers = client.list_dashboard_versions(
            AwsAccountId=ACCOUNT, DashboardId=DASHBOARD_ID, MaxResults=100
        )
        latest = max(v["VersionNumber"] for v in vers["DashboardVersionSummaryList"])
        row = next(
            v for v in vers["DashboardVersionSummaryList"] if v["VersionNumber"] == latest
        )
        st = row.get("Status")
        if st == "CREATION_SUCCESSFUL":
            break
        if st == "CREATION_FAILED":
            raise RuntimeError(f"Dashboard version {latest} failed: {row}")
        time.sleep(2)
    else:
        raise TimeoutError("Timed out waiting for dashboard version")

    client.update_dashboard_published_version(
        AwsAccountId=ACCOUNT,
        DashboardId=DASHBOARD_ID,
        VersionNumber=latest,
    )
    print(f"Published dashboard version {latest}")


if __name__ == "__main__":
    main()
