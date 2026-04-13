from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import streamlit as st

DATA_GOLD = Path("data/gold")
DATA_GOLD_LEGACY = Path("data/gold_legacy")
COMPARISON_TOTALS = Path("data/derived/comparison/posted_month_source_totals/part-00001.parquet")
HEALTH_ARBEITNOW = Path("data/health/latest_ingest.json")
HEALTH_ADZUNA = Path("data/health/latest_ingest_adzuna_in.json")


def _gold_source_root(table: str, source: str) -> Path:
    """Modular layout: data/gold/<table>/source=<slug>/. Legacy: data/gold/<table>/ (arbeitnow-only)."""
    v2 = DATA_GOLD / table / f"source={source}"
    if v2.exists():
        return v2
    if source == "arbeitnow":
        legacy = DATA_GOLD / table
        if legacy.exists():
            return legacy
    return v2


def _latest_run_id_for_source(source: str) -> str | None:
    meta = DATA_GOLD / f"source={source}" / "latest_run_metadata" / "part-00001.parquet"
    if not meta.exists():
        return None
    try:
        m = pd.read_parquet(meta)
        if m is not None and not m.empty and "run_id" in m.columns:
            return str(m["run_id"].iloc[0])
    except Exception:
        return None
    return None


def _month_key_from_path(path: Path) -> str | None:
    for part in path.parts:
        if part.startswith("posted_month="):
            return part.split("=", 1)[1]
        if part.startswith("ingest_month="):
            return part.split("=", 1)[1]
    return None


def _glob_posted_month_parts(root: Path, run_id: str | None) -> list[Path]:
    if not root.exists():
        return []
    if run_id:
        pm = sorted(root.glob(f"posted_month=*/run_id={run_id}/part-*.parquet"))
        if pm:
            return pm
    return sorted(root.glob("posted_month=*/run_id=*/part-*.parquet"))


def _glob_legacy_ingest_parts(table: str, source: str, run_id: str | None) -> list[Path]:
    """Local mirror of archived s3 gold_legacy/<table>/... ingest_month=... (optional)."""
    root = DATA_GOLD_LEGACY / table / f"source={source}"
    if not root.exists():
        return []
    if run_id:
        im = sorted(root.glob(f"ingest_month=*/run_id={run_id}/part-*.parquet"))
        if im:
            return im
    return sorted(root.glob("ingest_month=*/run_id=*/part-*.parquet"))


def _list_fact_paths(table: str, source: str, run_id: str | None) -> tuple[list[Path], str]:
    """Active Gold uses posted_month= under data/gold/...; ingest_month only under data/gold_legacy/..."""
    root = _gold_source_root(table, source)
    paths = _glob_posted_month_parts(root, run_id)
    if paths:
        return paths, "posted_month"
    leg = _glob_legacy_ingest_parts(table, source, run_id)
    if leg:
        return leg, "ingest_month (gold_legacy)"
    return [], "posted_month"


def _pick_parquet_for_month(paths: list[Path], posted_month: str) -> Path | None:
    for p in paths:
        if _month_key_from_path(p) == posted_month:
            return p
    return paths[-1] if paths else None


def _available_months(paths: list[Path]) -> list[str]:
    keys = [_month_key_from_path(p) for p in paths]
    return sorted({k for k in keys if k}, reverse=True)

TABLE_TOP = 20
CHART_TOP = 12
TABLE_LABEL_MAX = 88
CHART_LABEL_MAX = 38
ROLE_TABLE_HARD_MAX = 100


def _collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())


def _truncate_display(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _smart_shorten(s: str, max_len: int) -> str:
    """Truncate at a word boundary when possible (display only)."""
    s = s.strip()
    if len(s) <= max_len:
        return s
    cut = s[: max_len - 1].rsplit(" ", 1)[0]
    if len(cut) < max_len // 2:
        return s[: max_len - 1] + "…"
    return cut + "…"


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


_LEGAL_SUFFIX_RE = re.compile(
    r"\s*,?\s*(GmbH|AG|SE|Ltd\.?|Inc\.?|S\.A\.|S\.p\.A\.|PLC|LLC|B\.V\.|N\.V\.|KG|UG)\s*$",
    re.IGNORECASE,
)


def _strip_company_legal_suffix(s: str) -> str:
    """Remove trailing legal form from company display (identity words stay)."""
    s = _collapse_ws(s)
    prev = None
    while prev != s:
        prev = s
        s = _LEGAL_SUFFIX_RE.sub("", s)
        s = _collapse_ws(s)
    return s


def _fix_company_tokens(s: str) -> str:
    s = re.sub(r"\bGmbh\b", "GmbH", s)
    s = re.sub(r"\bAg\b", "AG", s)
    s = re.sub(r"\bSe\b", "SE", s)
    s = re.sub(r"\bUk\b", "UK", s)
    s = re.sub(r"\bUsa\b", "USA", s)
    return s


def format_company_display(raw: object) -> str:
    """Polished company label for tables: casing + drop legal suffix from visible name."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = _collapse_ws(str(raw))
    if not s:
        return ""
    s = s.title()
    s = _fix_company_tokens(s)
    s = _strip_company_legal_suffix(s)
    return s


def format_company_chart(raw: object) -> str:
    """Short chart label: same polish as table, then compact truncate."""
    s = format_company_display(raw)
    if not s:
        return ""
    return _smart_shorten(s, CHART_LABEL_MAX)


_GENDER_PAREN_RE = re.compile(
    r"\s*\(\s*[mfwd]\s*/\s*[mfwd]\s*/\s*[mfwd]\s*\)",
    re.IGNORECASE,
)


def polish_role_title(raw: object) -> str:
    """Human-friendly role line for tables (display only; does not invent categories)."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = _collapse_ws(str(raw))
    if not s:
        return ""

    # Drop noisy job-board tails (keep left of @)
    if "@" in s:
        s = s.split("@", 1)[0].strip()

    # Remove common parenthetical gender / diversity noise
    s = _GENDER_PAREN_RE.sub("", s)
    s = re.sub(r"\s*\([^)]*\ball\s+genders\b[^)]*\)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*\(all\s+genders\)\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\bihk\b", "", s, flags=re.IGNORECASE)

    # (Senior) Foo / (Senior)Foo -> Senior Foo
    s = re.sub(r"^\(\s*Senior\s*\)\s*", "Senior ", s, flags=re.IGNORECASE)
    s = re.sub(r"\(\s*Senior\s*\)(?=\S)", "Senior ", s, flags=re.IGNORECASE)

    # Training prefix: keep core job name
    s = re.sub(r"^ausbildung\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"fachinformatiker\*?in", "Fachinformatiker", s, flags=re.IGNORECASE)

    # Gender-star forms like Fachinformatiker*in
    s = re.sub(r"(\w)\*([iI]n)\b", r"\1", s)

    # Schwerpunkt: "Role - Schwerpunkt X" -> "Role - X"
    s = re.sub(r"\s*-\s*Schwerpunkt\s+", " - ", s, flags=re.IGNORECASE)

    # Normalize slashes and hyphens spacing
    s = re.sub(r"\s*[/\\]\s*", " / ", s)
    s = re.sub(r"\s*-\s*", " - ", s)
    s = _collapse_ws(s)

    # Title case for dashboard readability (German job titles)
    s = s.title()

    if len(s) > ROLE_TABLE_HARD_MAX:
        s = _smart_shorten(s, ROLE_TABLE_HARD_MAX)

    return s


def polish_role_chart(raw: object) -> str:
    """Shorter role label for charts (polished title, then compact)."""
    s = polish_role_title(raw)
    if not s:
        return ""
    return _smart_shorten(s, CHART_LABEL_MAX)


def format_role_display(raw: object) -> str:
    """Backward-compatible name: table polish."""
    return polish_role_title(raw)


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


def _display_differs_from_raw(raw_val: object, display: str) -> bool:
    r = _collapse_ws(str(raw_val)).lower()
    d = _collapse_ws(display).lower()
    return r != d


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
    table_part["Label"] = table_part["_display"].map(
        lambda s: _truncate_display(s, TABLE_LABEL_MAX) if s else ""
    )

    show_original = False
    if label_kind in ("role", "company", "location"):
        table_part["_show_orig"] = table_part.apply(
            lambda r: _display_differs_from_raw(r[name_col], str(r["_display"])),
            axis=1,
        )
        show_original = bool(table_part["_show_orig"].any())
        if show_original:
            table_part["Original"] = table_part.apply(
                lambda r: r[name_col] if r["_show_orig"] else None,
                axis=1,
            )

    out_cols = ["Label", "job_count"]
    if show_original:
        out_cols = ["Label", "Original", "job_count"]

    if label_kind == "role":
        chart_part["_chart_label"] = chart_part[name_col].map(polish_role_chart)
    elif label_kind == "company":
        chart_part["_chart_label"] = chart_part[name_col].map(format_company_chart)
    else:
        chart_part["_chart_label"] = chart_part[name_col].map(fmt)
        chart_part["_chart_label"] = chart_part["_chart_label"].map(
            lambda s: _truncate_display(s, CHART_LABEL_MAX) if s else ""
        )
    chart_part["_chart_label"] = chart_part["_chart_label"].fillna("")
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


st.set_page_config(page_title="JMI Dashboard", layout="wide", initial_sidebar_state="expanded")

st.sidebar.markdown("### Analysis scope")
analysis_mode = st.sidebar.radio(
    "Dataset",
    ["arbeitnow", "adzuna_in", "comparison"],
    format_func=lambda x: {
        "arbeitnow": "Arbeitnow (base Gold)",
        "adzuna_in": "Adzuna India (base Gold)",
        "comparison": "Comparison / benchmark (derived)",
    }[x],
    horizontal=False,
)

st.markdown("## Job Market Intelligence")
st.caption(
    "Local analytics: **posted_month** = job posting month (from Silver `posted_at`). "
    "Comparison totals live under `data/derived/comparison/`."
)

if analysis_mode == "comparison":
    st.markdown("### Cross-source comparison (derived)")
    st.caption(f"Source file: `{COMPARISON_TOTALS}` — job counts by **posted_month** × **source** from merged Silver.")
    if not COMPARISON_TOTALS.exists():
        st.warning(
            "No comparison file yet. Run **Gold** (it refreshes `derived/comparison/`) or "
            "`python -m src.jmi.pipelines.transform_derived_comparison`."
        )
        st.stop()
    df_cmp = _read_parquet(COMPARISON_TOTALS)
    if df_cmp is None or df_cmp.empty:
        st.info("Comparison file is empty.")
        st.stop()
    pm_sel = st.sidebar.selectbox(
        "Posted month",
        sorted(df_cmp["posted_month"].unique().tolist(), reverse=True),
    )
    sub = df_cmp[df_cmp["posted_month"] == pm_sel]
    st.dataframe(sub, use_container_width=True, hide_index=True)
    if len(sub) > 1:
        st.bar_chart(sub.set_index("source")["job_count"], height=280)
    st.stop()

rid = _latest_run_id_for_source(analysis_mode)
paths_skill, grain = _list_fact_paths("skill_demand_monthly", analysis_mode, rid)
if not paths_skill:
    st.info("No Gold datasets for this source yet. Run ingest → silver → gold.")
    st.stop()

months = _available_months(paths_skill)
if not months:
    st.warning("No month partitions found under Gold for this source/run.")
    st.stop()
posted_month_sel = st.sidebar.selectbox(
    "Posted month (analysis axis)",
    months,
    index=0,
    help="Counts are aggregated by the calendar month of **posted_at** in Silver (not pipeline ingest date).",
)
st.sidebar.caption(f"Partition key in paths: **{grain}**.")

path_skill = _pick_parquet_for_month(paths_skill, posted_month_sel)
paths_role, _ = _list_fact_paths("role_demand_monthly", analysis_mode, rid)
paths_loc, _ = _list_fact_paths("location_demand_monthly", analysis_mode, rid)
paths_co, _ = _list_fact_paths("company_hiring_monthly", analysis_mode, rid)
paths_sum, _ = _list_fact_paths("pipeline_run_summary", analysis_mode, rid)
path_role = _pick_parquet_for_month(paths_role, posted_month_sel)
path_location = _pick_parquet_for_month(paths_loc, posted_month_sel)
path_company = _pick_parquet_for_month(paths_co, posted_month_sel)
path_summary = _pick_parquet_for_month(paths_sum, posted_month_sel)

df_skill = _read_parquet(path_skill)
df_role = _read_parquet(path_role)
df_location = _read_parquet(path_location)
df_company = _read_parquet(path_company)
df_summary = _read_parquet(path_summary)

health: dict = {}
if HEALTH_ARBEITNOW.exists():
    try:
        health = json.loads(HEALTH_ARBEITNOW.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        health = {}
health_adzuna: dict = {}
if HEALTH_ADZUNA.exists():
    try:
        health_adzuna = json.loads(HEALTH_ADZUNA.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        health_adzuna = {}

if not any([path_skill, path_role, path_location, path_company, path_summary]):
    st.info("No Gold datasets yet. Run ingest, silver, and gold transforms.")
    st.stop()

skill_rows = int(len(df_skill)) if df_skill is not None and not df_skill.empty else 0
role_rows = int(len(df_role)) if df_role is not None and not df_role.empty else 0
location_rows = int(len(df_location)) if df_location is not None and not df_location.empty else 0
company_rows = int(len(df_company)) if df_company is not None and not df_company.empty else 0

pipeline_status = "—"
if df_summary is not None and not df_summary.empty and "status" in df_summary.columns:
    pipeline_status = str(df_summary["status"].iloc[0])

st.markdown("### Overview")
st.caption(f"Source **{analysis_mode}** · analysis month **{posted_month_sel}** · path grain **{grain}**")
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
            if "time_axis" in row.index:
                _kv("Time axis mix", row.get("time_axis"))
    with right:
        st.markdown("**Status**")
        status_val = str(row.get("status", "—"))
        if status_val.upper() == "PASS":
            st.success(status_val)
        elif status_val != "—":
            st.error(status_val)
        else:
            st.info("—")
        st.caption("From `pipeline_run_summary` for the selected **posted_month** partition.")
else:
    st.warning("No `pipeline_run_summary` data found. Run the gold transform.")

st.divider()

st.markdown("### Freshness & run metadata")
ing_left, ing_right = st.columns(2, gap="large")
with ing_left:
    with st.container(border=True):
        st.markdown("**Ingest health**")
        h = health_adzuna if analysis_mode == "adzuna_in" else health
        cap = "`data/health/latest_ingest_adzuna_in.json`" if analysis_mode == "adzuna_in" else "`data/health/latest_ingest.json`"
        st.caption(cap)
        _kv("Source", h.get("source"))
        _kv("Last run id", h.get("run_id"))
        _kv("Bronze ingest date", h.get("bronze_ingest_date"))
        _kv("Batch created at", h.get("batch_created_at"))
        _kv("Bronze record count", h.get("record_count"))
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
