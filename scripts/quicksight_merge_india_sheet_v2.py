#!/usr/bin/env python3
"""
Append the India (Adzuna) sheet to the v2 production QuickSight analysis and dashboard.

Reads live definition via describe_*_definition, clones EU visuals with dataset remaps,
and calls update-analysis / update-dashboard. Idempotent: replaces sheet named INDIA_SHEET_NAME.

Does not modify v1 assets or Athena/Glue.
"""

from __future__ import annotations

import copy
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import boto3

REGION = "ap-south-1"
ACCOUNT = "470441577506"
ANALYSIS_ID = "jmi-v2-analysis-production-eu"
DASHBOARD_ID = "jmi-v2-dashboard-production"

INDIA_SHEET_NAME = "India — Adzuna market structure"

# Short identifiers + production dataset ARNs (ap-south-1)
INDIA_DATASETS: List[Tuple[str, str]] = [
    ("in_location_top15", "arn:aws:quicksight:ap-south-1:470441577506:dataset/194e0eda-d3d9-4c96-80ee-5ac8ba30347e"),
    ("in_skill_latest", "arn:aws:quicksight:ap-south-1:470441577506:dataset/7066d81e-830f-4839-b563-3115a212e24a"),
    ("in_role_pareto", "arn:aws:quicksight:ap-south-1:470441577506:dataset/f4ea2471-f1f5-4ece-9504-5b3f70a7886c"),
    ("in_role_top20", "arn:aws:quicksight:ap-south-1:470441577506:dataset/9416917e-9913-4b40-bf40-99214e4cffc0"),
    ("in_company", "arn:aws:quicksight:ap-south-1:470441577506:dataset/ebf1f9c8-d551-4dcd-ba8b-ea7ceac5cd09"),
    ("in_pipeline", "arn:aws:quicksight:ap-south-1:470441577506:dataset/28fc92ed-5144-4070-be78-652c82643952"),
]

# EU template visual ids on sheet "market intelligence & structural evaluation"
EU_VISUAL_IDS = {
    "pie": "a9a66973-4db5-476e-bedc-5bede7f76d1c",
    "location_bar": "e99e3ad3-f533-4aec-b23d-b120fa570709",
    "combo": "b07c69bb-e2bb-4a23-9f26-bbafc6a37524",
    "role_bar": "e3840008-b05a-4788-94f1-8d321e2c7951",
}

EU_PIPELINE_TABLE_ID = "8d81ba7f-20e9-49d2-9b0d-49cbcf65b55a"


def _new_id() -> str:
    return str(uuid.uuid4())


def _remap_datasets(obj: Any, repl: Dict[str, str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "DataSetIdentifier" and isinstance(v, str) and v in repl:
                obj[k] = repl[v]
            else:
                _remap_datasets(v, repl)
    elif isinstance(obj, list):
        for x in obj:
            _remap_datasets(x, repl)


def _remap_field_ids(obj: Any, field_map: Dict[str, str]) -> None:
    if isinstance(obj, dict):
        if "FieldId" in obj and isinstance(obj["FieldId"], str):
            old = obj["FieldId"]
            if old not in field_map:
                field_map[old] = f"in-{uuid.uuid4().hex[:10]}.{uuid.uuid4().hex[:8]}"
            obj["FieldId"] = field_map[old]
        for v in obj.values():
            _remap_field_ids(v, field_map)
    elif isinstance(obj, list):
        for x in obj:
            _remap_field_ids(x, field_map)


def _find_visual(sheet: Dict[str, Any], visual_id: str) -> Dict[str, Any]:
    for v in sheet["Visuals"]:
        for _k, inner in v.items():
            if isinstance(inner, dict) and inner.get("VisualId") == visual_id:
                return v
    raise KeyError(f"Visual {visual_id} not found")


def _extract_inner(visual_wrapper: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    (kind,) = tuple(visual_wrapper.keys())
    return kind, copy.deepcopy(visual_wrapper[kind])


def _clone_visual(
    template_inner: Dict[str, Any],
    kind: str,
    dataset_repl: Dict[str, str],
    visual_id: str,
    title_richtext: Optional[str] = None,
    subtitle_richtext: Optional[str] = None,
) -> Dict[str, Any]:
    inner = copy.deepcopy(template_inner)
    inner["VisualId"] = visual_id
    fm: Dict[str, str] = {}
    _remap_datasets(inner, dataset_repl)
    _remap_field_ids(inner, fm)
    if title_richtext and "Title" in inner and inner["Title"].get("Visibility") == "VISIBLE":
        inner["Title"]["FormatText"]["RichText"] = title_richtext
    if subtitle_richtext and "Subtitle" in inner and inner["Subtitle"].get("Visibility") == "VISIBLE":
        inner["Subtitle"]["FormatText"]["RichText"] = subtitle_richtext
    return {kind: inner}


def _remap_column_name(obj: Any, dataset_id: str, old: str, new: str) -> None:
    if isinstance(obj, dict):
        if "Column" in obj and isinstance(obj["Column"], dict):
            c = obj["Column"]
            if c.get("DataSetIdentifier") == dataset_id and c.get("ColumnName") == old:
                c["ColumnName"] = new
        for v in obj.values():
            _remap_column_name(v, dataset_id, old, new)
    elif isinstance(obj, list):
        for x in obj:
            _remap_column_name(x, dataset_id, old, new)


def merge_india_sheet(defn: Dict[str, Any]) -> Dict[str, Any]:
    """Return a new definition dict with India sheet and dataset declarations."""
    out = copy.deepcopy(defn)

    # Drop existing India sheet if present
    out["Sheets"] = [s for s in out["Sheets"] if s.get("Name") != INDIA_SHEET_NAME]

    decls = out.setdefault("DataSetIdentifierDeclarations", [])
    have = {d["Identifier"] for d in decls}
    for ident, arn in INDIA_DATASETS:
        if ident not in have:
            decls.append({"Identifier": ident, "DataSetArn": arn})
            have.add(ident)

    sheet1 = next(s for s in out["Sheets"] if s["Name"] == "market intelligence & structural evaluation")
    sheet2 = next(s for s in out["Sheets"] if "pipeline" in s.get("Name", "").lower())

    pie_w = _find_visual(sheet1, EU_VISUAL_IDS["pie"])
    loc_w = _find_visual(sheet1, EU_VISUAL_IDS["location_bar"])
    combo_w = _find_visual(sheet1, EU_VISUAL_IDS["combo"])
    role_w = _find_visual(sheet1, EU_VISUAL_IDS["role_bar"])
    pipe_w = _find_visual(sheet2, EU_PIPELINE_TABLE_ID)

    k_pie, pie_t = _extract_inner(pie_w)
    k_loc, loc_t = _extract_inner(loc_w)
    k_combo, combo_t = _extract_inner(combo_w)
    k_role, role_t = _extract_inner(role_w)
    k_pipe, pipe_t = _extract_inner(pipe_w)

    vid_pie = _new_id()
    vid_loc = _new_id()
    vid_combo = _new_id()
    vid_role = _new_id()
    vid_company = _new_id()
    vid_pipe = _new_id()
    tb_id = _new_id()
    sheet_id = _new_id()

    v_pie = _clone_visual(
        pie_t,
        k_pie,
        {"skill_demand_monthly_latest": "in_skill_latest"},
        vid_pie,
        "<visual-title>Skill tag composition (India / Adzuna)</visual-title>",
        "<visual-subtitle>Skill tags from the latest Adzuna India run (non-additive; one job may list multiple skills).</visual-subtitle>",
    )

    v_loc = _clone_visual(
        loc_t,
        k_loc,
        {"location_top15_other": "in_location_top15"},
        vid_loc,
        "<visual-title>Location mix — India (Top 15 + Other)</visual-title>",
        "<visual-subtitle>Horizontal bars ∝ posting volume by location bucket (geo-structured view; not a choropleth).</visual-subtitle>",
    )

    v_combo = _clone_visual(
        combo_t,
        k_combo,
        {"role_group_pareto": "in_role_pareto"},
        vid_combo,
        "<visual-title>Role families — Pareto coverage (India)</visual-title>",
        "<visual-subtitle>Bars = postings per grouped role family; line = cumulative % of total postings.</visual-subtitle>",
    )

    v_role = _clone_visual(
        role_t,
        k_role,
        {"role_group_top20": "in_role_top20"},
        vid_role,
        "<visual-title>Top role families (India)</visual-title>",
        "<visual-subtitle>Grouped role categories from classified titles (Adzuna India).</visual-subtitle>",
    )

    # Company: clone location bar → company_label on in_company
    company_inner = copy.deepcopy(loc_t)
    company_inner["VisualId"] = vid_company
    _remap_datasets(company_inner, {"location_top15_other": "in_company"})
    _remap_column_name(company_inner, "in_company", "location_label", "company_label")
    fm_c: Dict[str, str] = {}
    _remap_field_ids(company_inner, fm_c)
    company_inner["Title"]["FormatText"]["RichText"] = (
        "<visual-title>Employer concentration (Top 50 + tail)</visual-title>"
    )
    company_inner["Subtitle"]["FormatText"]["RichText"] = (
        "<visual-subtitle>Posting volume by normalized employer label (India / Adzuna).</visual-subtitle>"
    )
    v_company = {"BarChartVisual": company_inner}

    pipe_inner = copy.deepcopy(pipe_t)
    pipe_inner["VisualId"] = vid_pipe
    _remap_datasets(pipe_inner, {"JMI_FINAL_pipeline_run_summary_latest": "in_pipeline"})
    fm_p: Dict[str, str] = {}
    _remap_field_ids(pipe_inner, fm_p)
    pipe_inner["Title"]["FormatText"]["RichText"] = (
        "<visual-title>Pipeline summary — Adzuna India</visual-title>"
    )
    v_pipe = {"TableVisual": pipe_inner}

    header = (
        "<text-box>\n"
        "  <block align=\"center\">\n"
        "    <inline font-size=\"32px\">\n"
        "      <b>India — Adzuna market structure</b>\n"
        "    </inline>\n"
        "  </block>\n"
        "  <br/>\n"
        "  <block align=\"center\">\n"
        "    <inline font-size=\"18px\">Geo buckets, skills, role structure, employer mix, and pipeline proof — "
        "distinct from the EU hero-KPI layout.</inline>\n"
        "  </block>\n"
        "</text-box>"
    )

    india_sheet: Dict[str, Any] = {
        "SheetId": sheet_id,
        "Name": INDIA_SHEET_NAME,
        "Visuals": [
            v_pie,
            v_loc,
            v_combo,
            v_role,
            v_company,
            v_pipe,
        ],
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
                                "Height": "120px",
                                "Visibility": "VISIBLE",
                            },
                            {
                                "ElementId": vid_loc,
                                "ElementType": "VISUAL",
                                "XAxisLocation": "0px",
                                "YAxisLocation": "128px",
                                "Width": "1596px",
                                "Height": "400px",
                                "Visibility": "VISIBLE",
                            },
                            {
                                "ElementId": vid_pie,
                                "ElementType": "VISUAL",
                                "XAxisLocation": "0px",
                                "YAxisLocation": "544px",
                                "Width": "780px",
                                "Height": "440px",
                                "Visibility": "VISIBLE",
                            },
                            {
                                "ElementId": vid_combo,
                                "ElementType": "VISUAL",
                                "XAxisLocation": "800px",
                                "YAxisLocation": "544px",
                                "Width": "796px",
                                "Height": "440px",
                                "Visibility": "VISIBLE",
                            },
                            {
                                "ElementId": vid_role,
                                "ElementType": "VISUAL",
                                "XAxisLocation": "0px",
                                "YAxisLocation": "1000px",
                                "Width": "780px",
                                "Height": "400px",
                                "Visibility": "VISIBLE",
                            },
                            {
                                "ElementId": vid_company,
                                "ElementType": "VISUAL",
                                "XAxisLocation": "800px",
                                "YAxisLocation": "1000px",
                                "Width": "796px",
                                "Height": "400px",
                                "Visibility": "VISIBLE",
                            },
                            {
                                "ElementId": vid_pipe,
                                "ElementType": "VISUAL",
                                "XAxisLocation": "0px",
                                "YAxisLocation": "1420px",
                                "Width": "1596px",
                                "Height": "220px",
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

    out["Sheets"].append(india_sheet)

    cols = out.setdefault("ColumnConfigurations", [])
    if not any(
        c.get("Column", {}).get("DataSetIdentifier") == "in_role_pareto"
        and c.get("Column", {}).get("ColumnName") == "pareto_rank"
        for c in cols
    ):
        cols.append(
            {
                "Column": {"DataSetIdentifier": "in_role_pareto", "ColumnName": "pareto_rank"},
                "Role": "DIMENSION",
            }
        )

    return out


def merge_dashboard_from_analysis(
    analysis_definition: Dict[str, Any], dashboard_definition: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Dashboard EU layouts differ from the analysis (e.g. TreeMap vs bar). Copy the India sheet
    from the analysis definition so visuals stay identical to the published analysis.
    """
    out = copy.deepcopy(dashboard_definition)
    out["Sheets"] = [s for s in out["Sheets"] if s.get("Name") != INDIA_SHEET_NAME]

    india_sheet = next(
        s for s in analysis_definition["Sheets"] if s.get("Name") == INDIA_SHEET_NAME
    )
    out["Sheets"].append(copy.deepcopy(india_sheet))

    decls = out.setdefault("DataSetIdentifierDeclarations", [])
    have = {d["Identifier"] for d in decls}
    for ident, arn in INDIA_DATASETS:
        if ident not in have:
            decls.append({"Identifier": ident, "DataSetArn": arn})

    cols = out.setdefault("ColumnConfigurations", [])
    if not any(
        c.get("Column", {}).get("DataSetIdentifier") == "in_role_pareto"
        and c.get("Column", {}).get("ColumnName") == "pareto_rank"
        for c in cols
    ):
        cols.append(
            {
                "Column": {"DataSetIdentifier": "in_role_pareto", "ColumnName": "pareto_rank"},
                "Role": "DIMENSION",
            }
        )
    return out


def main() -> None:
    client = boto3.client("quicksight", region_name=REGION)

    a = client.describe_analysis_definition(AwsAccountId=ACCOUNT, AnalysisId=ANALYSIS_ID)
    name_a = a["Name"]
    merged_a = merge_india_sheet(a["Definition"])
    client.update_analysis(
        AwsAccountId=ACCOUNT,
        AnalysisId=ANALYSIS_ID,
        Name=name_a,
        Definition=merged_a,
    )
    print(f"Updated analysis {ANALYSIS_ID}")

    # Use the merged definition we just sent (avoids rare post-update describe races).
    d = client.describe_dashboard_definition(AwsAccountId=ACCOUNT, DashboardId=DASHBOARD_ID)
    name_d = d["Name"]
    merged_d = merge_dashboard_from_analysis(merged_a, d["Definition"])
    client.update_dashboard(
        AwsAccountId=ACCOUNT,
        DashboardId=DASHBOARD_ID,
        Name=name_d,
        Definition=merged_d,
        VersionDescription="Add India Adzuna sheet",
    )
    print(f"Updated dashboard draft {DASHBOARD_ID}")

    # Wait until the newest version finishes building (avoids ConflictException).
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
            raise RuntimeError(f"Dashboard version {latest} failed to build: {row}")
        time.sleep(2)
    else:
        raise TimeoutError("Timed out waiting for dashboard version to become CREATION_SUCCESSFUL")

    pub = client.update_dashboard_published_version(
        AwsAccountId=ACCOUNT,
        DashboardId=DASHBOARD_ID,
        VersionNumber=latest,
    )
    print(f"Published dashboard version {latest}", pub.get("DashboardArn", pub))


if __name__ == "__main__":
    main()
