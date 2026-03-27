from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT_SKILL = Path("data/gold/skill_demand_monthly")
ROOT_ROLE = Path("data/gold/role_demand_monthly")
ROOT_LOCATION = Path("data/gold/location_demand_monthly")
ROOT_COMPANY = Path("data/gold/company_hiring_monthly")
ROOT_SUMMARY = Path("data/gold/pipeline_run_summary")
HEALTH_FILE = Path("data/health/latest_ingest.json")

TABLE_TOP = 20
CHART_TOP = 12
TABLE_LABEL_MAX = 72
CHART_LABEL_MAX = 34


def _collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())


def _truncate_display(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _dedupe_labels(labels: pd.Series) -> pd.Series:
    """Ensure unique labels for charts when formatting maps two rows to the same string."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for v in labels.astype(str):
        if not v:
            out.append(v)
            continue
        seen[v] = seen.get(v, 0) + 1
        if seen[v] > 1:
            out.append(f"{v} ({seen[v]})")
        else:
            out.append(v)
    return pd.Series(out, index=labels.index)


def format_role_display(raw: object) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = _collapse_ws(str(raw))
    if not s:
        return ""
    s = re.sub(r"\s*\((?:m/w/d|f/m/d|w/m/d|m/f/d)\)\s*$", "", s, flags=re.IGNORECASE)
    s = _collapse_ws(s)
    s = re.sub(r"\s*[/\\]\s*", " / ", s)
    s = re.sub(r"\s*-\s*", " - ", s)
    s = re.sub(r"\s+", " ", s)
    s = s.title()
    return s


def format_company_display(raw: object) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = _collapse_ws(str(raw))
    if not s:
        return ""
    s = s.title()
    s = re.sub(r"\bGmbh\b", "GmbH", s)
    s = re.sub(r"\bAg\b", "AG", s)
    s = re.sub(r"\bSe\b", "SE", s)
    s = re.sub(r"\bUk\b", "UK", s)
    s = re.sub(r"\bUsa\b", "USA", s)
    return s


def _company_label_for_chart(display_full: str) -> str:
    """Chart-only: shorten by dropping common legal suffixes for readability."""
    s = display_full.strip()
    s = re.sub(
        r"\s*,?\s*(GmbH|AG|SE|Ltd\.?|Inc\.?|S\.A\.|S\.p\.A\.|PLC|LLC)\s*$",
        "",
        s,
        flags=re.IGNORECASE,
    )
    return _collapse_ws(s)


def format_location_display(raw: object) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = _collapse_ws(str(raw))
    if not s:
        return ""
    return s.title()


def format_skill_display(raw: object) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = _collapse_ws(str(raw))
    if not s:
        return ""
    return s.title()


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


def _kv(label: str, value: object) -> None:
    c1, c2 = st.columns([0.35, 0.65])
    with c1:
        st.caption(label)
    with c2:
        st.markdown(str(value) if value is not None else "—")


def _render_ranked_section(
    title: str,
    df: pd.DataFrame | None,
    path: Path | None,
    name_col: str,
    missing_msg: str,
    *,
    label_kind: str,
) -> None:
    st.markdown(f"#### {title}")
    if path is not None:
        st.caption(f"Source: `{path}`")
    if df is None or df.empty or name_col not in df.columns:
        st.warning(missing_msg)
        return

    fmt_map = {
        "skill": format_skill_display,
        "role": format_role_display,
        "location": format_location_display,
        "company": format_company_display,
    }
    fmt = fmt_map.get(label_kind, lambda x: str(x) if x is not None else "")

    sorted_df = df.sort_values("job_count", ascending=False)
    table_part = sorted_df.head(TABLE_TOP).copy()
    chart_part = sorted_df.head(CHART_TOP).copy()

    table_part["_display"] = table_part[name_col].map(fmt)
    table_part["_display"] = table_part["_display"].fillna("")
    show_raw_col = label_kind in ("role", "company", "location")
    table_part["Label"] = table_part["_display"].map(
        lambda s: _truncate_display(s, TABLE_LABEL_MAX) if s else ""
    )
    out_cols = ["Label", "job_count"]
    if show_raw_col:
        table_part["As stored"] = table_part[name_col].astype(str)
        out_cols = ["Label", "As stored", "job_count"]

    chart_part["_chart_base"] = chart_part[name_col].map(fmt)
    if label_kind == "company":
        chart_part["_chart_label"] = chart_part["_chart_base"].map(_company_label_for_chart)
    else:
        chart_part["_chart_label"] = chart_part["_chart_base"]
    chart_part["_chart_label"] = chart_part["_chart_label"].map(
        lambda s: _truncate_display(s, CHART_LABEL_MAX) if s else ""
    )
    chart_part["_chart_label"] = _dedupe_labels(chart_part["_chart_label"])

    left, right = st.columns([1, 1], gap="large")
    with left:
        st.markdown("**Rankings**")
        st.dataframe(
            table_part[out_cols],
            use_container_width=True,
            hide_index=True,
        )
        st.caption("Labels are formatted for display only; counts and rankings use underlying Gold data.")
    with right:
        st.markdown("**Top counts (chart)**")
        st.bar_chart(
            chart_part.set_index("_chart_label")["job_count"],
            height=320,
        )


st.set_page_config(page_title="JMI Dashboard", layout="wide", initial_sidebar_state="collapsed")

st.markdown("## Job Market Intelligence")
st.caption("Local Gold analytics — skills, roles, locations, and hiring signals from the latest pipeline run.")

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

st.markdown("### Overview")
with st.container(border=True):
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Skill rows", f"{skill_rows:,}")
    k2.metric("Role rows", f"{role_rows:,}")
    k3.metric("Location rows", f"{location_rows:,}")
    k4.metric("Company rows", f"{company_rows:,}")
    k5.metric("Pipeline", pipeline_status)

st.divider()

st.markdown("### Pipeline summary / health")
if df_summary is not None and not df_summary.empty:
    row = df_summary.iloc[0]
    left, right = st.columns([1.1, 0.9], gap="large")
    with left:
        with st.container(border=True):
            st.markdown("**Run details**")
            _kv("Source", row.get("source"))
            _kv("Bronze ingest date", row.get("bronze_ingest_date"))
            _kv("Bronze run id", row.get("bronze_run_id"))
            st.markdown("**Gold row counts (by table)**")
            _kv("Skills", row.get("skill_row_count"))
            _kv("Roles", row.get("role_row_count"))
            _kv("Locations", row.get("location_row_count"))
            _kv("Companies", row.get("company_row_count"))
    with right:
        st.markdown("**Status**")
        status_val = str(row.get("status", "—"))
        if status_val.upper() == "PASS":
            st.success(status_val)
        elif status_val != "—":
            st.error(status_val)
        else:
            st.info("—")
        st.caption("From `pipeline_run_summary` (latest partition).")
else:
    st.warning("No `pipeline_run_summary` data found. Run the gold transform.")

st.divider()

st.markdown("### Freshness & run metadata")
ing_left, ing_right = st.columns(2, gap="large")
with ing_left:
    with st.container(border=True):
        st.markdown("**Ingest health**")
        st.caption("`data/health/latest_ingest.json`")
        _kv("Source", health.get("source"))
        _kv("Last run id", health.get("run_id"))
        _kv("Bronze ingest date", health.get("bronze_ingest_date"))
        _kv("Batch created at", health.get("batch_created_at"))
        _kv("Bronze record count", health.get("record_count"))
with ing_right:
    with st.container(border=True):
        st.markdown("**Gold lineage**")
        ref_df = df_skill if df_skill is not None and not df_skill.empty else df_summary
        ref_path = path_skill if path_skill else path_summary
        _kv("Gold file (reference)", str(ref_path) if ref_path else "—")
        if ref_df is not None and not ref_df.empty:
            _kv("Source", ref_df["source"].iloc[0] if "source" in ref_df.columns else None)
            _kv("Bronze run id", ref_df["bronze_run_id"].iloc[0] if "bronze_run_id" in ref_df.columns else None)
            _kv("Bronze ingest date", ref_df["bronze_ingest_date"].iloc[0] if "bronze_ingest_date" in ref_df.columns else None)
        else:
            st.caption("No skill or summary parquet for lineage.")

st.divider()

_render_ranked_section(
    "Top skills",
    df_skill,
    path_skill,
    "skill",
    "No `skill_demand_monthly` data yet.",
    label_kind="skill",
)
st.divider()
_render_ranked_section(
    "Top roles",
    df_role,
    path_role,
    "role",
    "No `role_demand_monthly` data yet.",
    label_kind="role",
)
st.divider()
_render_ranked_section(
    "Top locations",
    df_location,
    path_location,
    "location",
    "No `location_demand_monthly` data yet.",
    label_kind="location",
)
st.divider()
_render_ranked_section(
    "Top companies",
    df_company,
    path_company,
    "company_name",
    "No `company_hiring_monthly` data yet.",
    label_kind="company",
)
