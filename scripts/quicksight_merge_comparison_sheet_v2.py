#!/usr/bin/env python3
"""
Append or replace the Comparison benchmark sheet on v2 production analysis + dashboard.

Uses Athena-backed datasets:
  - comparison_source_month_totals (trend)
  - comparison_source_skill_mix_aligned_top20 (100% stacked mix)
  - comparison_benchmark_aligned_month (2-row proof table)

Resolves dataset ARNs by display name (no hardcoded dataset ids).
"""
from __future__ import annotations

import copy
import time
import uuid
from typing import Any, Dict, List, Tuple

import boto3

REGION = "ap-south-1"
ACCOUNT = "470441577506"
ANALYSIS_ID = "jmi-v2-analysis-production-eu"
DASHBOARD_ID = "jmi-v2-dashboard-production"

COMPARISON_SHEET_NAME = "Comparison — benchmark (Arbeitnow vs Adzuna)"

# (QuickSight identifier in definition, dataset display name)
CMP_DATASET_NAMES: List[Tuple[str, str]] = [
    ("cmp_month", "JMI v2 — comparison_source_month_totals"),
    ("cmp_skill_mix", "JMI v2 — comparison_source_skill_mix_aligned_top20"),
    ("cmp_benchmark", "JMI v2 — comparison_benchmark_aligned_month"),
]


def _new_field_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}.{uuid.uuid4().hex[:8]}"


def _resolve_dataset_arns(client) -> List[Tuple[str, str]]:
    """Return [(identifier, arn), ...] in stable order."""
    names_needed = {n for _, n in CMP_DATASET_NAMES}
    found: Dict[str, str] = {}
    token = None
    while True:
        kwargs: Dict[str, Any] = {"AwsAccountId": ACCOUNT, "MaxResults": 100}
        if token:
            kwargs["NextToken"] = token
        resp = client.list_data_sets(**kwargs)
        for s in resp.get("DataSetSummaries", []):
            nm = s.get("Name")
            if nm in names_needed and nm not in found:
                found[nm] = s["Arn"]
        token = resp.get("NextToken")
        if len(found) == len(names_needed):
            break
        if not token:
            break
    missing = names_needed - set(found.keys())
    if missing:
        raise RuntimeError(
            "Missing QuickSight datasets (run scripts/quicksight_create_comparison_datasets_v2.py): "
            + ", ".join(sorted(missing))
        )
    return [(ident, found[name]) for ident, name in CMP_DATASET_NAMES]


def _merge_comparison_sheet(defn: Dict[str, Any], cmp_decls: List[Tuple[str, str]]) -> Dict[str, Any]:
    out = copy.deepcopy(defn)
    out["Sheets"] = [s for s in out["Sheets"] if s.get("Name") != COMPARISON_SHEET_NAME]
    # Remove legacy comparison sheet name if present from older deploys
    out["Sheets"] = [s for s in out["Sheets"] if s.get("Name") != "Comparison — benchmark (EU vs IN)"]

    decls = out.setdefault("DataSetIdentifierDeclarations", [])
    # Replace any prior cmp_* bindings (dataset ids may change when helpers are renamed).
    decls[:] = [d for d in decls if not d["Identifier"].startswith("cmp_")]
    for ident, arn in cmp_decls:
        decls.append({"Identifier": ident, "DataSetArn": arn})

    sheet_id = str(uuid.uuid4())
    tb_id = str(uuid.uuid4())
    vid_line = str(uuid.uuid4())
    vid_bar = str(uuid.uuid4())
    vid_tbl = str(uuid.uuid4())

    f_month = _new_field_id("cmp_m")
    f_posts = _new_field_id("cmp_p")
    f_src_line = _new_field_id("cmp_src")
    f_src_bar = _new_field_id("cmp_sb")
    f_share = _new_field_id("cmp_sh")
    f_skill = _new_field_id("cmp_sk")

    # Benchmark table field ids (stable prefix for sort)
    fb_src = _new_field_id("cmp_b_src")
    fb_mo = _new_field_id("cmp_b_mo")
    fb_rid = _new_field_id("cmp_b_rid")
    fb_tot = _new_field_id("cmp_b_tot")
    fb_hhi = _new_field_id("cmp_b_hhi")

    header = (
        "<text-box>\n"
        "  <block align=\"center\">\n"
        "    <inline font-size=\"32px\">\n"
        "      <b>Benchmark — gold sources (not geography)</b>\n"
        "    </inline>\n"
        "  </block>\n"
        "  <br/>\n"
        "  <block align=\"center\">\n"
        "    <inline font-size=\"18px\">Arbeitnow vs Adzuna India at the same grain as gold tables. "
        "Volume = role buckets; skill = tag-demand (not deduped per job). "
        "This sheet is intentionally neutral — not the EU or India story layouts.</inline>\n"
        "  </block>\n"
        "</text-box>"
    )

    line_visual = {
        "LineChartVisual": {
            "VisualId": vid_line,
            "Title": {
                "Visibility": "VISIBLE",
                "FormatText": {
                    "RichText": "<visual-title>Posting volume by month (role grain)</visual-title>"
                },
            },
            "Subtitle": {
                "Visibility": "VISIBLE",
                "FormatText": {
                    "RichText": "<visual-subtitle>SUM(role_demand_monthly.job_count) by ingest_month. "
                    "Separate latest runs per source.</visual-subtitle>"
                },
            },
            "ChartConfiguration": {
                "FieldWells": {
                    "LineChartAggregatedFieldWells": {
                        "Category": [
                            {
                                "CategoricalDimensionField": {
                                    "FieldId": f_month,
                                    "Column": {
                                        "DataSetIdentifier": "cmp_month",
                                        "ColumnName": "ingest_month",
                                    },
                                }
                            }
                        ],
                        "Values": [
                            {
                                "NumericalMeasureField": {
                                    "FieldId": f_posts,
                                    "Column": {
                                        "DataSetIdentifier": "cmp_month",
                                        "ColumnName": "total_postings",
                                    },
                                    "AggregationFunction": {
                                        "SimpleNumericalAggregation": "SUM"
                                    },
                                }
                            }
                        ],
                        "Colors": [
                            {
                                "CategoricalDimensionField": {
                                    "FieldId": f_src_line,
                                    "Column": {
                                        "DataSetIdentifier": "cmp_month",
                                        "ColumnName": "source",
                                    },
                                }
                            }
                        ],
                    }
                },
                "SortConfiguration": {
                    "CategorySort": [
                        {
                            "FieldSort": {
                                "FieldId": f_month,
                                "Direction": "ASC",
                            }
                        }
                    ]
                },
                "Legend": {"Width": "200px"},
                "DataLabels": {"Visibility": "HIDDEN", "Overlap": "DISABLE_OVERLAP"},
                "Tooltip": {
                    "TooltipVisibility": "VISIBLE",
                    "SelectedTooltipType": "DETAILED",
                },
            },
            "Actions": [],
        }
    }

    bar_visual = {
        "BarChartVisual": {
            "VisualId": vid_bar,
            "Title": {
                "Visibility": "VISIBLE",
                "FormatText": {
                    "RichText": "<visual-title>Skill tag mix — top 20, aligned month</visual-title>"
                },
            },
            "Subtitle": {
                "Visibility": "VISIBLE",
                "FormatText": {
                    "RichText": "<visual-subtitle>100% stacked within top-20 tag slice per source; "
                    "aligned to latest month present in both sources.</visual-subtitle>"
                },
            },
            "ChartConfiguration": {
                "FieldWells": {
                    "BarChartAggregatedFieldWells": {
                        "Category": [
                            {
                                "CategoricalDimensionField": {
                                    "FieldId": f_src_bar,
                                    "Column": {
                                        "DataSetIdentifier": "cmp_skill_mix",
                                        "ColumnName": "source",
                                    },
                                }
                            }
                        ],
                        "Values": [
                            {
                                "NumericalMeasureField": {
                                    "FieldId": f_share,
                                    "Column": {
                                        "DataSetIdentifier": "cmp_skill_mix",
                                        "ColumnName": "share_within_source_skill_tags",
                                    },
                                    "AggregationFunction": {
                                        "SimpleNumericalAggregation": "SUM"
                                    },
                                }
                            }
                        ],
                        "Colors": [
                            {
                                "CategoricalDimensionField": {
                                    "FieldId": f_skill,
                                    "Column": {
                                        "DataSetIdentifier": "cmp_skill_mix",
                                        "ColumnName": "skill",
                                    },
                                }
                            }
                        ],
                    }
                },
                "SortConfiguration": {
                    "CategorySort": [
                        {
                            "FieldSort": {
                                "FieldId": f_src_bar,
                                "Direction": "ASC",
                            }
                        }
                    ]
                },
                "Orientation": "VERTICAL",
                "BarsArrangement": "STACKED_PERCENT",
                "Legend": {"Width": "300px"},
                "DataLabels": {"Visibility": "HIDDEN", "Overlap": "DISABLE_OVERLAP"},
                "Tooltip": {
                    "TooltipVisibility": "VISIBLE",
                    "SelectedTooltipType": "DETAILED",
                },
            },
            "Actions": [],
        }
    }

    table_visual = {
        "TableVisual": {
            "VisualId": vid_tbl,
            "Title": {
                "Visibility": "VISIBLE",
                "FormatText": {
                    "RichText": "<visual-title>Aligned month — proof row</visual-title>"
                },
            },
            "Subtitle": {
                "Visibility": "VISIBLE",
                "FormatText": {
                    "RichText": "<visual-subtitle>Role postings + skill-tag HHI for the same calendar month when both sources overlap.</visual-subtitle>"
                },
            },
            "ChartConfiguration": {
                "FieldWells": {
                    "TableUnaggregatedFieldWells": {
                        "Values": [
                            {
                                "FieldId": fb_src,
                                "Column": {
                                    "DataSetIdentifier": "cmp_benchmark",
                                    "ColumnName": "source",
                                },
                            },
                            {
                                "FieldId": fb_mo,
                                "Column": {
                                    "DataSetIdentifier": "cmp_benchmark",
                                    "ColumnName": "aligned_ingest_month",
                                },
                            },
                            {
                                "FieldId": fb_rid,
                                "Column": {
                                    "DataSetIdentifier": "cmp_benchmark",
                                    "ColumnName": "run_id",
                                },
                            },
                            {
                                "FieldId": fb_tot,
                                "Column": {
                                    "DataSetIdentifier": "cmp_benchmark",
                                    "ColumnName": "total_role_postings",
                                },
                            },
                            {
                                "FieldId": fb_hhi,
                                "Column": {
                                    "DataSetIdentifier": "cmp_benchmark",
                                    "ColumnName": "skill_tag_hhi",
                                },
                            },
                        ]
                    }
                },
                "SortConfiguration": {
                    "RowSort": [
                        {
                            "FieldSort": {
                                "FieldId": fb_src,
                                "Direction": "ASC",
                            }
                        }
                    ]
                },
                "TableOptions": {
                    "HeaderStyle": {
                        "TextWrap": "WRAP",
                        "Height": 25,
                    }
                },
            },
            "Actions": [],
        }
    }

    sheet: Dict[str, Any] = {
        "SheetId": sheet_id,
        "Name": COMPARISON_SHEET_NAME,
        "Visuals": [line_visual, bar_visual, table_visual],
        "TextBoxes": [{"SheetTextBoxId": tb_id, "Content": header}],
        "Images": [],
        "Layouts": [
            {
                "Configuration": {
                    "FreeFormLayout": {
                        "Elements": [
                            {
                                "ElementId": tb_id,
                                "ElementType": "TEXT_BOX",
                                "XAxisLocation": "0px",
                                "YAxisLocation": "0px",
                                "Width": "1596px",
                                "Height": "150px",
                                "Visibility": "VISIBLE",
                            },
                            {
                                "ElementId": vid_line,
                                "ElementType": "VISUAL",
                                "XAxisLocation": "0px",
                                "YAxisLocation": "160px",
                                "Width": "1596px",
                                "Height": "440px",
                                "Visibility": "VISIBLE",
                            },
                            {
                                "ElementId": vid_bar,
                                "ElementType": "VISUAL",
                                "XAxisLocation": "0px",
                                "YAxisLocation": "616px",
                                "Width": "1596px",
                                "Height": "480px",
                                "Visibility": "VISIBLE",
                            },
                            {
                                "ElementId": vid_tbl,
                                "ElementType": "VISUAL",
                                "XAxisLocation": "0px",
                                "YAxisLocation": "1112px",
                                "Width": "1596px",
                                "Height": "200px",
                                "Visibility": "VISIBLE",
                            },
                        ],
                        "CanvasSizeOptions": {
                            "ScreenCanvasSizeOptions": {"OptimizedViewPortWidth": "1600px"}
                        },
                    }
                }
            }
        ],
        "ContentType": "INTERACTIVE",
    }

    out["Sheets"].append(sheet)
    return out


def merge_dashboard_from_analysis(
    analysis_definition: Dict[str, Any], dashboard_definition: Dict[str, Any]
) -> Dict[str, Any]:
    out = copy.deepcopy(dashboard_definition)
    for legacy in (
        COMPARISON_SHEET_NAME,
        "Comparison — benchmark (EU vs IN)",
    ):
        out["Sheets"] = [s for s in out["Sheets"] if s.get("Name") != legacy]

    cmp_sheet = next(
        s for s in analysis_definition["Sheets"] if s.get("Name") == COMPARISON_SHEET_NAME
    )
    out["Sheets"].append(copy.deepcopy(cmp_sheet))

    decls = out.setdefault("DataSetIdentifierDeclarations", [])
    decls[:] = [d for d in decls if not d["Identifier"].startswith("cmp_")]
    for ident, arn in _decls_from_analysis(analysis_definition):
        decls.append({"Identifier": ident, "DataSetArn": arn})
    return out


def _decls_from_analysis(analysis_definition: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Extract cmp_* declarations from merged analysis definition."""
    out = []
    for d in analysis_definition.get("DataSetIdentifierDeclarations", []):
        if d["Identifier"].startswith("cmp_"):
            out.append((d["Identifier"], d["DataSetArn"]))
    return out


def main() -> None:
    client = boto3.client("quicksight", region_name=REGION)

    cmp_decls = _resolve_dataset_arns(client)
    print("Declarations:", cmp_decls, flush=True)

    a = client.describe_analysis_definition(AwsAccountId=ACCOUNT, AnalysisId=ANALYSIS_ID)
    merged_a = _merge_comparison_sheet(a["Definition"], cmp_decls)
    client.update_analysis(
        AwsAccountId=ACCOUNT,
        AnalysisId=ANALYSIS_ID,
        Name=a["Name"],
        Definition=merged_a,
    )
    print(f"Updated analysis {ANALYSIS_ID}")

    d = client.describe_dashboard_definition(AwsAccountId=ACCOUNT, DashboardId=DASHBOARD_ID)
    merged_d = merge_dashboard_from_analysis(merged_a, d["Definition"])
    client.update_dashboard(
        AwsAccountId=ACCOUNT,
        DashboardId=DASHBOARD_ID,
        Name=d["Name"],
        Definition=merged_d,
        VersionDescription="Comparison sheet: Arbeitnow vs Adzuna (source helpers)",
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
