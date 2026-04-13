#!/usr/bin/env python3
"""
Targeted copy/label polish for jmi-v2-analysis-production-eu + jmi-v2-dashboard-production.

- Sharpens Europe / India / comparison sheet identity in headers and a few chart titles.
- Adds pipeline table subtitle; fixes stale jmi_analytics.* path strings in subtitles.
- Does not alter datasets, filters, or visual types.

Run: python3 scripts/quicksight_polish_v2_definitions.py  (requires boto3)
"""
from __future__ import annotations

import copy
import json
import time
from typing import Any, Dict

import boto3

REGION = "ap-south-1"
ACCOUNT = "470441577506"
ANALYSIS_ID = "jmi-v2-analysis-production-eu"
DASHBOARD_ID = "jmi-v2-dashboard-production"


def _patch_eu_market_sheet(sheet: Dict[str, Any]) -> int:
    n = 0
    for tb in sheet.get("TextBoxes", []):
        c = tb.get("Content", "")
        if "Arbeitnow job market snapshot" not in c:
            continue
        tb["Content"] = (
            c.replace(
                "<b>Arbeitnow job market snapshot — structural view</b>",
                "<b>Europe (Arbeitnow) — structural snapshot</b>",
            )
            .replace(
                "Single-month gold aggregates from a validated pipeline run. This page describes how posting mass is distributed across skills, locations, titles, and employers — not trends over time.",
                "Single-month gold for the latest Europe run. How posting mass is distributed across skills, locations, grouped roles, and employers — not a time-series trend page.",
            )
        )
        n += 1
    for v in sheet.get("Visuals", []):
        pie = v.get("PieChartVisual")
        if not pie:
            continue
        rt = pie.get("Title", {}).get("FormatText", {}).get("RichText", "")
        if rt.strip() == "<visual-title>Skill tag composition</visual-title>":
            pie["Title"]["FormatText"]["RichText"] = (
                "<visual-title>Skill tag composition (Europe)</visual-title>"
            )
            n += 1
    return n


def _patch_india_sheet(sheet: Dict[str, Any]) -> int:
    n = 0
    for tb in sheet.get("TextBoxes", []):
        c = tb.get("Content", "")
        if "India — Adzuna market structure" not in c:
            continue
        tb["Content"] = (
            c.replace(
                "<b>India — Adzuna market structure</b>",
                "<b>India (Adzuna) — geography &amp; structure</b>",
            )
            .replace(
                "Geo buckets, skills, role structure, employer mix, and pipeline proof — distinct from the EU hero-KPI layout.",
                "Location buckets, skills, role families, employers, and pipeline proof — built for distribution and structure (contrasts with the Europe KPI row).",
            )
        )
        n += 1
    for v in sheet.get("Visuals", []):
        pie = v.get("PieChartVisual")
        if not pie:
            continue
        rt = pie.get("Title", {}).get("FormatText", {}).get("RichText", "")
        if "India" in rt and "Skill tag" in rt:
            pie["Title"]["FormatText"]["RichText"] = "<visual-title>Skill tag demand (India)</visual-title>"
            pie["Subtitle"]["FormatText"]["RichText"] = (
                "<visual-subtitle>Tag-level demand from the latest Adzuna India run "
                "(non-additive; one job may list multiple skills).</visual-subtitle>"
            )
            n += 1
    return n


def _patch_platform_sheet(sheet: Dict[str, Any]) -> int:
    n = 0
    for v in sheet.get("Visuals", []):
        tv = v.get("TableVisual")
        if not tv:
            continue
        title = tv.get("Title", {}).get("FormatText", {}).get("RichText", "")
        if "Pipeline summary" not in title:
            continue
        tv.setdefault("Subtitle", {})["Visibility"] = "VISIBLE"
        tv["Subtitle"]["FormatText"] = {
            "RichText": "<visual-subtitle>Latest run: dataset row counts and status (v2 gold / analytics path).</visual-subtitle>"
        }
        tv["Title"]["FormatText"]["RichText"] = "<visual-title>Pipeline summary (latest run)</visual-title>"
        n += 1
    for tb in sheet.get("TextBoxes", []):
        c = tb.get("Content", "")
        if "pipeline_run_summary_latest" not in c and "Pipeline" not in c:
            continue
        if "jmi_analytics.pipeline_run_summary_latest" in c:
            tb["Content"] = c.replace(
                "jmi_analytics.pipeline_run_summary_latest",
                "jmi_analytics_v2 (pipeline summary view)",
            )
            n += 1
    return n


def _patch_comparison_sheet(sheet: Dict[str, Any]) -> int:
    n = 0
    for tb in sheet.get("TextBoxes", []):
        c = tb.get("Content", "")
        if "Benchmark — gold sources" not in c:
            continue
        tb["Content"] = (
            c.replace(
                "Arbeitnow vs Adzuna India at the same grain as gold tables.",
                "Arbeitnow (Europe feed) vs Adzuna India at the same gold grain.",
            )
        )
        n += 1
    return n


def _replace_stale_catalog_strings(obj: Any) -> int:
    """In-place replace known stale subtitle paths. Returns change count."""
    n = 0
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("RichText", "Content") and isinstance(v, str):
                if "jmi_analytics.role_group_top20" in v:
                    obj[k] = v.replace(
                        "jmi_analytics.role_group_top20",
                        "jmi_analytics_v2.role_group_pareto",
                    )
                    n += 1
            else:
                n += _replace_stale_catalog_strings(v)
    elif isinstance(obj, list):
        for x in obj:
            n += _replace_stale_catalog_strings(x)
    return n


def polish_definition(defn: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(defn)
    total = 0
    for sheet in out.get("Sheets", []):
        name = sheet.get("Name", "")
        if name == "market intelligence & structural evaluation":
            total += _patch_eu_market_sheet(sheet)
        elif name == "India — Adzuna market structure":
            total += _patch_india_sheet(sheet)
        elif "pipeline" in name.lower():
            total += _patch_platform_sheet(sheet)
        elif "Comparison — benchmark" in name:
            total += _patch_comparison_sheet(sheet)
    total += _replace_stale_catalog_strings(out)
    return out, total


def main() -> None:
    client = boto3.client("quicksight", region_name=REGION)

    a = client.describe_analysis_definition(AwsAccountId=ACCOUNT, AnalysisId=ANALYSIS_ID)
    polished_a, n_a = polish_definition(a["Definition"])
    print(f"Analysis polish patches (approx): {n_a}")

    client.update_analysis(
        AwsAccountId=ACCOUNT,
        AnalysisId=ANALYSIS_ID,
        Name=a["Name"],
        Definition=polished_a,
    )
    print(f"Updated analysis {ANALYSIS_ID}")

    d = client.describe_dashboard_definition(AwsAccountId=ACCOUNT, DashboardId=DASHBOARD_ID)
    # Polish dashboard separately — do not copy sheets from analysis (SheetIds differ; FilterGroups reference dashboard SheetIds).
    merged_d, n_d = polish_definition(d["Definition"])
    print(f"Dashboard polish patches (approx): {n_d}")

    client.update_dashboard(
        AwsAccountId=ACCOUNT,
        DashboardId=DASHBOARD_ID,
        Name=d["Name"],
        Definition=merged_d,
        VersionDescription="Polish: sheet copy, subtitles, v2 catalog strings",
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
