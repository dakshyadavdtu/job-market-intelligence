from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT_SKILL = Path("data/gold/skill_demand_monthly")
ROOT_ROLE = Path("data/gold/role_demand_monthly")
ROOT_LOCATION = Path("data/gold/location_demand_monthly")
ROOT_COMPANY = Path("data/gold/company_hiring_monthly")
ROOT_SUMMARY = Path("data/gold/pipeline_run_summary")
HEALTH_FILE = Path("data/health/latest_ingest.json")


def _latest_parquet(root: Path) -> Path | None:
    files = sorted(root.glob("ingest_month=*/run_id=*/part-*.parquet"))
    return files[-1] if files else None


def _read_parquet(path: Path | None) -> pd.DataFrame | None:
    if path is None or not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


st.set_page_config(page_title="JMI Dashboard", layout="wide")
st.title("Job Market Intelligence (MVP)")

health: dict = {}
if HEALTH_FILE.exists():
    try:
        health = json.loads(HEALTH_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        health = {}

path_skill = _latest_parquet(ROOT_SKILL)
path_role = _latest_parquet(ROOT_ROLE)
path_location = _latest_parquet(ROOT_LOCATION)
path_company = _latest_parquet(ROOT_COMPANY)
path_summary = _latest_parquet(ROOT_SUMMARY)

if not any([path_skill, path_role, path_location, path_company, path_summary]):
    st.info("No Gold datasets yet. Run ingest, silver, and gold transforms.")
    st.stop()

df_skill = _read_parquet(path_skill)
df_role = _read_parquet(path_role)
df_location = _read_parquet(path_location)
df_company = _read_parquet(path_company)
df_summary = _read_parquet(path_summary)

skill_rows = int(len(df_skill)) if df_skill is not None and not df_skill.empty else 0
role_rows = int(len(df_role)) if df_role is not None and not df_role.empty else 0
location_rows = int(len(df_location)) if df_location is not None and not df_location.empty else 0
company_rows = int(len(df_company)) if df_company is not None and not df_company.empty else 0

pipeline_status = "—"
if df_summary is not None and not df_summary.empty and "status" in df_summary.columns:
    pipeline_status = str(df_summary["status"].iloc[0])

st.subheader("Overview")
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Skill rows (gold)", skill_rows)
m2.metric("Role rows (gold)", role_rows)
m3.metric("Location rows (gold)", location_rows)
m4.metric("Company rows (gold)", company_rows)
m5.metric("Pipeline status", pipeline_status)

st.subheader("Pipeline summary / health")
if df_summary is not None and not df_summary.empty:
    row = df_summary.iloc[0].to_dict()
    st.caption("Latest `pipeline_run_summary` parquet.")
    st.write(
        {
            "source": row.get("source"),
            "bronze_ingest_date": row.get("bronze_ingest_date"),
            "bronze_run_id": row.get("bronze_run_id"),
            "skill_row_count": row.get("skill_row_count"),
            "role_row_count": row.get("role_row_count"),
            "location_row_count": row.get("location_row_count"),
            "company_row_count": row.get("company_row_count"),
            "status": row.get("status"),
        }
    )
else:
    st.warning("No `pipeline_run_summary` data found. Run the gold transform.")

st.subheader("Freshness and run metadata")
meta_col1, meta_col2 = st.columns(2)
with meta_col1:
    st.caption("Ingest health file (`data/health/latest_ingest.json`).")
    st.write(
        {
            "source": health.get("source"),
            "last_run_id": health.get("run_id"),
            "bronze_ingest_date": health.get("bronze_ingest_date"),
            "batch_created_at": health.get("batch_created_at"),
            "bronze_record_count": health.get("record_count"),
        }
    )
with meta_col2:
    ref_df = df_skill if df_skill is not None and not df_skill.empty else df_summary
    st.caption("Lineage from latest skill gold file (or summary if skills missing).")
    if ref_df is not None and not ref_df.empty:
        st.write(
            {
                "gold_file": str(path_skill) if path_skill else str(path_summary),
                "gold_source": str(ref_df["source"].iloc[0]) if "source" in ref_df.columns else None,
                "gold_bronze_run_id": str(ref_df["bronze_run_id"].iloc[0])
                if "bronze_run_id" in ref_df.columns
                else None,
                "gold_bronze_ingest_date": str(ref_df["bronze_ingest_date"].iloc[0])
                if "bronze_ingest_date" in ref_df.columns
                else None,
            }
        )
    else:
        st.write({"gold_file": None, "note": "No skill or summary parquet for lineage."})

st.subheader("Top skills")
if df_skill is not None and not df_skill.empty and "skill" in df_skill.columns:
    st.caption(f"File: `{path_skill}`")
    top_skills = df_skill.sort_values("job_count", ascending=False).head(20)
    st.dataframe(top_skills[["skill", "job_count"]], use_container_width=True)
    st.bar_chart(top_skills.set_index("skill")["job_count"])
else:
    st.warning("No `skill_demand_monthly` data yet.")

st.subheader("Top roles")
if df_role is not None and not df_role.empty and "role" in df_role.columns:
    st.caption(f"File: `{path_role}`")
    top_roles = df_role.sort_values("job_count", ascending=False).head(20)
    st.dataframe(top_roles[["role", "job_count"]], use_container_width=True)
    st.bar_chart(top_roles.set_index("role")["job_count"])
else:
    st.warning("No `role_demand_monthly` data yet.")

st.subheader("Top locations")
if df_location is not None and not df_location.empty and "location" in df_location.columns:
    st.caption(f"File: `{path_location}`")
    top_locs = df_location.sort_values("job_count", ascending=False).head(20)
    st.dataframe(top_locs[["location", "job_count"]], use_container_width=True)
    st.bar_chart(top_locs.set_index("location")["job_count"])
else:
    st.warning("No `location_demand_monthly` data yet.")

st.subheader("Top companies")
if df_company is not None and not df_company.empty and "company_name" in df_company.columns:
    st.caption(f"File: `{path_company}`")
    top_cos = df_company.sort_values("job_count", ascending=False).head(20)
    st.dataframe(top_cos[["company_name", "job_count"]], use_container_width=True)
    st.bar_chart(top_cos.set_index("company_name")["job_count"])
else:
    st.warning("No `company_hiring_monthly` data yet.")
