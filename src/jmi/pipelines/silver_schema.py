"""Canonical Silver column names and legacy -> canonical alignment for merges."""

from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd

_WS = re.compile(r"\s+")
_HTML_TAG = re.compile(r"<[^>]+>")
_HTML_SCRIPT = re.compile(r"(?is)<script[^>]*>.*?</script>")
_HTML_STYLE = re.compile(r"(?is)<style[^>]*>.*?</style>")


def normalize_title_norm(title: str) -> str:
    t = (title or "").strip()
    if not t:
        return ""
    return _WS.sub(" ", t).lower()


def normalize_company_norm(company: str) -> str:
    c = (company or "").strip()
    if not c:
        return ""
    return _WS.sub(" ", c).lower()


def strip_html_description(raw: str) -> str:
    if not raw or not str(raw).strip():
        return ""
    t = html.unescape(str(raw))
    t = _HTML_SCRIPT.sub(" ", t)
    t = _HTML_STYLE.sub(" ", t)
    t = _HTML_TAG.sub(" ", t)
    return _WS.sub(" ", t).strip()


def split_location_city_country(location_raw: str) -> tuple[str | None, str | None]:
    loc = (location_raw or "").strip()
    if not loc:
        return None, None
    if "," not in loc:
        return loc, None
    parts = [p.strip() for p in loc.split(",") if p.strip()]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[-1]


def remote_type_from_arbeitnow_payload(payload: dict[str, Any]) -> str:
    r = payload.get("remote")
    if r is True:
        return "remote"
    if r is False:
        return "onsite"
    return "unknown"


def employment_type_from_arbeitnow_payload(payload: dict[str, Any]) -> str | None:
    jt = payload.get("job_types")
    if isinstance(jt, list) and jt:
        parts = [str(x).strip() for x in jt if str(x).strip()]
        return ", ".join(parts) if parts else None
    return None


def category_from_arbeitnow_tags(tags: list[str] | None) -> str | None:
    if not tags or not isinstance(tags, list):
        return None
    for t in tags:
        s = str(t).strip()
        if s:
            return s
    return None


def posted_at_iso_utc(payload: dict[str, Any]) -> str | None:
    ts = payload.get("created_at")
    if ts is None:
        return None
    try:
        if isinstance(ts, (int, float)):
            sec = int(ts)
        else:
            s = str(ts).strip()
            if not s or not s.isdigit():
                return None
            sec = int(s)
        return datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OSError, ValueError, OverflowError):
        return None


CANONICAL_SILVER_COLUMN_ORDER: list[str] = [
    "job_id",
    "source",
    "source_job_id",
    "title_raw",
    "title_norm",
    "company_raw",
    "company_norm",
    "location_raw",
    "location_city",
    "location_country",
    "remote_type",
    "employment_type",
    "category",
    "description_text",
    "skills",
    "salary_min",
    "salary_max",
    "salary_currency",
    "posted_at",
    "ingested_at",
    "record_status",
    "raw_url",
    "job_id_strategy",
    "schema_version",
    "source_record_key",
    "bronze_run_id",
    "bronze_ingest_date",
    "bronze_data_file",
]


def _legacy_source_job_id_from_key(key: object) -> str | None:
    k = str(key or "").strip()
    if not k or k.lower().startswith("http"):
        return None
    return k


def align_silver_dataframe_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """If df uses pre-canonical column names, map to canonical; else ensure column set/order."""
    if "title_raw" not in df.columns:
        out = _map_legacy_silver_to_canonical(df)
    else:
        out = df.copy()

    for col in CANONICAL_SILVER_COLUMN_ORDER:
        if col not in out.columns:
            if col in ("salary_min", "salary_max"):
                out[col] = pd.Series(pd.NA, index=out.index, dtype="Float64")
            elif col == "skills":
                out[col] = [[] for _ in range(len(out))]
            elif col == "record_status":
                out[col] = "active"
            elif col == "description_text":
                out[col] = ""
            else:
                out[col] = pd.NA

    ordered = [c for c in CANONICAL_SILVER_COLUMN_ORDER if c in out.columns]
    extra = [c for c in out.columns if c not in ordered]
    return out[ordered + extra].copy()


def _map_legacy_silver_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["job_id"] = df["job_id"] if "job_id" in df.columns else pd.NA
    out["source"] = df["source"] if "source" in df.columns else pd.NA
    out["job_id_strategy"] = df.get("job_id_strategy", "")
    out["schema_version"] = df.get("schema_version", "")
    out["source_record_key"] = df.get("source_record_key", "")

    title_clean = df["title_clean"].fillna("").astype(str) if "title_clean" in df.columns else pd.Series("", index=df.index)
    title_lower = df["title"].fillna("").astype(str) if "title" in df.columns else pd.Series("", index=df.index)
    out["title_raw"] = title_clean
    out["title_norm"] = title_lower.where(
        title_lower.str.len() > 0,
        title_clean.map(normalize_title_norm),
    )

    cname = df["company_name"].fillna("").astype(str) if "company_name" in df.columns else pd.Series("", index=df.index)
    out["company_raw"] = cname
    out["company_norm"] = cname.str.lower().str.strip().str.replace(_WS, " ", regex=True)

    loc = df["location"].fillna("").astype(str) if "location" in df.columns else pd.Series("", index=df.index)
    out["location_raw"] = loc

    def _city_country(cell: str) -> tuple[str | None, str | None]:
        return split_location_city_country(cell) if cell else (None, None)

    cc = loc.map(_city_country)
    out["location_city"] = cc.map(lambda x: x[0])
    out["location_country"] = cc.map(lambda x: x[1])

    if "is_remote" in df.columns:
        out["remote_type"] = df["is_remote"].map(
            lambda x: "remote" if x is True else ("onsite" if x is False else "unknown")
        )
    else:
        out["remote_type"] = "unknown"

    out["employment_type"] = pd.NA
    out["category"] = pd.NA
    out["description_text"] = ""
    out["skills"] = df["skills"] if "skills" in df.columns else [[] for _ in range(len(df))]
    out["salary_min"] = pd.Series([pd.NA] * len(df), dtype="Float64")
    out["salary_max"] = pd.Series([pd.NA] * len(df), dtype="Float64")
    out["salary_currency"] = pd.NA

    out["posted_at"] = df["published_at_raw"] if "published_at_raw" in df.columns else pd.NA
    out["ingested_at"] = df["ingested_at"] if "ingested_at" in df.columns else pd.NA
    out["record_status"] = "active"
    out["raw_url"] = df["posting_url"].fillna("").astype(str) if "posting_url" in df.columns else ""

    src_key = df["source_record_key"].fillna("").astype(str) if "source_record_key" in df.columns else pd.Series("", index=df.index)
    out["source_job_id"] = src_key.map(_legacy_source_job_id_from_key)
    out["bronze_run_id"] = df["bronze_run_id"] if "bronze_run_id" in df.columns else pd.NA
    out["bronze_ingest_date"] = df["bronze_ingest_date"] if "bronze_ingest_date" in df.columns else pd.NA
    out["bronze_data_file"] = df["bronze_data_file"] if "bronze_data_file" in df.columns else pd.NA

    return out
