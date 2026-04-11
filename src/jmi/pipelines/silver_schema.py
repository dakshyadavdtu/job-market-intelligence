"""Canonical Silver column names and legacy -> canonical alignment for merges."""

from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd

_WS = re.compile(r"\s+")
_COMMA_RUN_LOC = re.compile(r",+")
_SEGMENT_EDGE_PUNCT_LOC = re.compile(r"^[\s.,;:|/\\-]+|[\s.,;:|/\\-]+$")
_TITLE_GENDER_PAREN = re.compile(
    r"\s*\(\s*[mfwd]\s*/\s*[mfwd]\s*/\s*[mfwd]\s*\)",
    re.IGNORECASE,
)
_TITLE_EDGE_TRIM = re.compile(r"^[\s.,;:|/\\-]+|[\s.,;:|/\\-]+$")
_HTML_TAG = re.compile(r"<[^>]+>")
_HTML_SCRIPT = re.compile(r"(?is)<script[^>]*>.*?</script>")
_HTML_STYLE = re.compile(r"(?is)<style[^>]*>.*?</style>")


def normalize_title_norm(title: str) -> str:
    """Lowercase analytic title: collapse whitespace, drop common DE gender suffixes, trim edge punctuation."""
    t = (title or "").strip()
    if not t:
        return ""
    t = _WS.sub(" ", t)
    t = _TITLE_GENDER_PAREN.sub(" ", t)
    t = _WS.sub(" ", t).strip()
    t = _TITLE_EDGE_TRIM.sub("", t)
    return t.lower().strip()


def normalize_company_norm(company: str) -> str:
    c = (company or "").strip()
    if not c:
        return ""
    return _WS.sub(" ", c).lower()


_CANONICAL_LOCATION_ALIASES: dict[str, str] = {
    "frankfurt": "frankfurt am main",
}


def _clean_location_segment(raw: str) -> str:
    seg = _SEGMENT_EDGE_PUNCT_LOC.sub("", _WS.sub(" ", raw.strip()))
    return seg


def normalize_location_raw(value: object) -> str:
    """Shared Silver + Gold: clean comma-separated locations (dedupe segments, trim noise)."""
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = _WS.sub(" ", text)
    text = _COMMA_RUN_LOC.sub(",", text)
    parts: list[str] = []
    for raw_seg in text.split(","):
        seg = _clean_location_segment(raw_seg)
        if seg:
            parts.append(seg)
    if not parts:
        return ""
    if len(parts) >= 3 and parts[0] == parts[1]:
        parts = [parts[0]]
    deduped: list[str] = [parts[0]]
    for seg in parts[1:]:
        if seg != deduped[-1]:
            deduped.append(seg)
    if len(deduped) == 1:
        out = deduped[0]
    elif len(deduped) == 2 and deduped[0] == "berlin" and deduped[1] == "germany":
        out = "berlin"
    else:
        out = ", ".join(deduped)
    return _CANONICAL_LOCATION_ALIASES.get(out, out)


def strip_html_description(raw: str) -> str:
    if not raw or not str(raw).strip():
        return ""
    t = html.unescape(str(raw))
    t = _HTML_SCRIPT.sub(" ", t)
    t = _HTML_STYLE.sub(" ", t)
    t = _HTML_TAG.sub(" ", t)
    return _WS.sub(" ", t).strip()


def remote_type_from_arbeitnow_payload(payload: dict[str, Any]) -> str:
    r = payload.get("remote")
    if r is True:
        return "remote"
    if r is False:
        return "onsite"
    return "unknown"


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


# Minimal Silver: only columns Gold needs + essential lineage + canonical job facts (strict parquet contract).
CANONICAL_SILVER_COLUMN_ORDER: list[str] = [
    "job_id",
    "source",
    "source_job_id",
    "title_norm",
    "company_norm",
    "location_raw",
    "remote_type",
    "skills",
    "posted_at",
    "ingested_at",
    "job_id_strategy",
    "bronze_run_id",
    "bronze_ingest_date",
    "bronze_data_file",
]


def project_silver_to_contract(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce exact Silver contract: only CANONICAL columns, fixed order (strips legacy/extra parquet fields)."""
    out = pd.DataFrame(index=df.index)
    for c in CANONICAL_SILVER_COLUMN_ORDER:
        if c in df.columns:
            out[c] = df[c]
        elif c == "skills":
            out[c] = [[] for _ in range(len(df))]
        else:
            out[c] = pd.NA
    return out


def _legacy_source_job_id_from_key(key: object) -> str | None:
    k = str(key or "").strip()
    if not k or k.lower().startswith("http"):
        return None
    return k


def align_silver_dataframe_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """If df uses pre-canonical column names, map to canonical; else ensure column set/order."""
    if "title_norm" not in df.columns:
        out = _map_legacy_silver_to_canonical(df)
    else:
        out = df.copy()

    for col in CANONICAL_SILVER_COLUMN_ORDER:
        if col not in out.columns:
            if col == "skills":
                out[col] = [[] for _ in range(len(out))]
            else:
                out[col] = pd.NA

    out["title_norm"] = out["title_norm"].fillna("").astype(str).map(normalize_title_norm)
    out["location_raw"] = out["location_raw"].fillna("").astype(str).map(normalize_location_raw)

    return project_silver_to_contract(out)


def _map_legacy_silver_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["job_id"] = df["job_id"] if "job_id" in df.columns else pd.NA
    out["source"] = df["source"] if "source" in df.columns else pd.NA
    out["job_id_strategy"] = df.get("job_id_strategy", "")

    if "title_norm" in df.columns:
        out["title_norm"] = df["title_norm"].fillna("").astype(str).map(normalize_title_norm)
    elif "title_raw" in df.columns:
        out["title_norm"] = df["title_raw"].fillna("").astype(str).map(normalize_title_norm)
    else:
        title_clean = df["title_clean"].fillna("").astype(str) if "title_clean" in df.columns else pd.Series("", index=df.index)
        title_lower = df["title"].fillna("").astype(str) if "title" in df.columns else pd.Series("", index=df.index)
        out["title_norm"] = title_lower.where(
            title_lower.str.len() > 0,
            title_clean.map(normalize_title_norm),
        )

    if "company_norm" in df.columns:
        out["company_norm"] = df["company_norm"].fillna("").astype(str)
    elif "company_raw" in df.columns:
        out["company_norm"] = df["company_raw"].fillna("").astype(str).map(normalize_company_norm)
    else:
        cname = df["company_name"].fillna("").astype(str) if "company_name" in df.columns else pd.Series("", index=df.index)
        out["company_norm"] = cname.str.lower().str.strip().str.replace(_WS, " ", regex=True)

    if "location_raw" in df.columns:
        out["location_raw"] = df["location_raw"].fillna("").astype(str).map(normalize_location_raw)
    elif "location" in df.columns:
        out["location_raw"] = df["location"].fillna("").astype(str).map(normalize_location_raw)
    else:
        out["location_raw"] = ""

    if "remote_type" in df.columns:
        out["remote_type"] = df["remote_type"]
    elif "is_remote" in df.columns:
        out["remote_type"] = df["is_remote"].map(
            lambda x: "remote" if x is True else ("onsite" if x is False else "unknown")
        )
    else:
        out["remote_type"] = "unknown"

    out["skills"] = df["skills"] if "skills" in df.columns else [[] for _ in range(len(df))]

    if "posted_at" in df.columns:
        out["posted_at"] = df["posted_at"]
    elif "published_at_raw" in df.columns:
        out["posted_at"] = df["published_at_raw"]
    else:
        out["posted_at"] = pd.NA

    out["ingested_at"] = df["ingested_at"] if "ingested_at" in df.columns else pd.NA

    if "source_job_id" in df.columns:
        out["source_job_id"] = df["source_job_id"]
    elif "source_record_key" in df.columns:
        src_key = df["source_record_key"].fillna("").astype(str)
        out["source_job_id"] = src_key.map(_legacy_source_job_id_from_key)
    else:
        out["source_job_id"] = pd.NA

    out["bronze_run_id"] = df["bronze_run_id"] if "bronze_run_id" in df.columns else pd.NA
    out["bronze_ingest_date"] = df["bronze_ingest_date"] if "bronze_ingest_date" in df.columns else pd.NA
    out["bronze_data_file"] = df["bronze_data_file"] if "bronze_data_file" in df.columns else pd.NA

    return out
