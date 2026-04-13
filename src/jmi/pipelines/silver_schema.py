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
    # Collapse stray slash-only fragments ("manager/" -> "manager")
    t = re.sub(r"\s*/\s*$", "", t).strip()
    t = re.sub(r"\s*/\s+", " ", t)
    return t.lower().strip()


def normalize_company_norm(company: str) -> str:
    c = (company or "").strip()
    if not c:
        return ""
    # Vendor display names sometimes use pipes ("Control One | AI") — normalize to spaces for analytics.
    c = c.replace("|", " ")
    c = _WS.sub(" ", c).lower()
    # Drop leading English article when it is clearly a company prefix ("The Sleep" -> "sleep")
    if c.startswith("the ") and len(c) > 8:
        c = c[4:].strip()
    c = _TITLE_EDGE_TRIM.sub("", c)
    return c.strip()


_CANONICAL_LOCATION_ALIASES: dict[str, str] = {
    "frankfurt": "frankfurt am main",
}

# India: normalize mixed vendor strings (city-only, city+state, state+country, country-only) to a small set of forms:
#   "{city}, {state}" | "{state}, india" | "{city}, india" (weak) | "india"
_INDIA_SEGMENT_ALIASES: dict[str, str] = {
    "orissa": "odisha",
    "bengaluru": "bangalore",
    "gurugram": "gurgaon",
}

_INDIA_STATES_AND_UTS: frozenset[str] = frozenset(
    {
        "andhra pradesh",
        "arunachal pradesh",
        "assam",
        "bihar",
        "chhattisgarh",
        "goa",
        "gujarat",
        "haryana",
        "himachal pradesh",
        "jharkhand",
        "karnataka",
        "kerala",
        "madhya pradesh",
        "maharashtra",
        "manipur",
        "meghalaya",
        "mizoram",
        "nagaland",
        "odisha",
        "punjab",
        "rajasthan",
        "sikkim",
        "tamil nadu",
        "telangana",
        "tripura",
        "uttar pradesh",
        "uttarakhand",
        "west bengal",
        "delhi",
        "jammu and kashmir",
        "ladakh",
        "puducherry",
        "chandigarh",
        "dadra and nagar haveli and daman and diu",
        "lakshadweep",
        "andaman and nicobar islands",
    }
)

# Major cities → state/UT for India (when API returns city-only or "city, india").
_INDIA_CITY_TO_STATE: dict[str, str] = {
    "mumbai": "maharashtra",
    "pune": "maharashtra",
    "nagpur": "maharashtra",
    "nashik": "maharashtra",
    "thane": "maharashtra",
    "navi mumbai": "maharashtra",
    "bangalore": "karnataka",
    "hyderabad": "telangana",
    "chennai": "tamil nadu",
    "coimbatore": "tamil nadu",
    "madurai": "tamil nadu",
    "kolkata": "west bengal",
    "ahmedabad": "gujarat",
    "surat": "gujarat",
    "vadodara": "gujarat",
    "jaipur": "rajasthan",
    "lucknow": "uttar pradesh",
    "kanpur": "uttar pradesh",
    "noida": "uttar pradesh",
    "ghaziabad": "uttar pradesh",
    "gurgaon": "haryana",
    "faridabad": "haryana",
    "indore": "madhya pradesh",
    "bhopal": "madhya pradesh",
    "kozhikode": "kerala",
    "kochi": "kerala",
    "thiruvananthapuram": "kerala",
    "visakhapatnam": "andhra pradesh",
    "vijayawada": "andhra pradesh",
    "patna": "bihar",
    "ranchi": "jharkhand",
    "bhubaneswar": "odisha",
    "guwahati": "assam",
    "chandigarh": "chandigarh",
    "mysore": "karnataka",
    "mysuru": "karnataka",
}


def _alias_india_segment(seg: str) -> str:
    s = seg.strip().lower()
    return _INDIA_SEGMENT_ALIASES.get(s, s)


def _is_india_location_context(parts: list[str]) -> bool:
    if not parts:
        return False
    for p in parts:
        if p in ("india", "in", "bharat"):
            return True
        if p in _INDIA_STATES_AND_UTS:
            return True
        if p in _INDIA_CITY_TO_STATE:
            return True
    return False


def _canonicalize_india_location_parts(parts: list[str]) -> str:
    """Return canonical India string: prefer city+state; else state+india; else india."""
    raw = [_alias_india_segment(p) for p in parts if p and str(p).strip()]
    if not raw:
        return ""
    dedup: list[str] = [raw[0]]
    for x in raw[1:]:
        if x != dedup[-1]:
            dedup.append(x)
    raw = dedup

    if len(raw) == 1:
        only = raw[0]
        if only in ("india", "in"):
            return "india"
        if only in _INDIA_STATES_AND_UTS:
            return f"{only}, india"
        if only in _INDIA_CITY_TO_STATE:
            return f"{only}, {_INDIA_CITY_TO_STATE[only]}"
        return only

    if len(raw) == 2:
        a, b = raw[0], raw[1]
        if b in ("india", "in"):
            if a in _INDIA_STATES_AND_UTS:
                return f"{a}, india"
            if a in _INDIA_CITY_TO_STATE:
                return f"{a}, {_INDIA_CITY_TO_STATE[a]}"
            return f"{a}, india"
        if b in _INDIA_STATES_AND_UTS:
            return f"{a}, {b}"
        return f"{a}, {b}"

    # 3+ segments: drop redundant trailing india after city, state
    if raw[-1] in ("india", "in") and raw[-2] in _INDIA_STATES_AND_UTS and len(raw) >= 3:
        if raw[-3] not in _INDIA_STATES_AND_UTS:
            return f"{raw[-3]}, {raw[-2]}"
    return ", ".join(raw)


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
    out = _CANONICAL_LOCATION_ALIASES.get(out, out)
    # India: unify mixed API shapes (city-only, city+state, state+india, india-only).
    parts_in = [_clean_location_segment(s) for s in out.split(",")]
    parts_in = [p for p in parts_in if p]
    if parts_in and _is_india_location_context(parts_in):
        canon = _canonicalize_india_location_parts(parts_in)
        if canon:
            return canon
    return out


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
    """Arbeitnow `created_at`: Unix seconds (int/str) or occasional ISO-8601 strings."""
    ts = payload.get("created_at")
    if ts is None:
        return None
    try:
        if isinstance(ts, (int, float)):
            sec = int(ts)
            return datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        s = str(ts).strip()
        if not s:
            return None
        if s.isdigit():
            sec = int(s)
            return datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OSError, ValueError, OverflowError, TypeError):
        return None


def posted_at_iso_adzuna_created(payload: dict[str, Any]) -> str | None:
    """Adzuna job search uses ISO 8601 string `created` (not Unix `created_at`)."""
    s = str(payload.get("created") or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError, OSError, OverflowError):
        return None


def posted_at_iso_from_payload(payload: dict[str, Any]) -> str | None:
    """Arbeitnow first (Unix created_at); then Adzuna (ISO created)."""
    return posted_at_iso_utc(payload) or posted_at_iso_adzuna_created(payload)


_ADZUNA_GENERIC_CATEGORY_LABELS: frozenset[str] = frozenset(
    {
        "it jobs",
        "jobs",
        "job",
        "general",
        "all jobs",
        "vacancies",
        "careers",
    }
)

# Category tags that are too vague to append to title_norm or skill context.
_ADZUNA_GENERIC_CATEGORY_TAGS: frozenset[str] = frozenset(
    {
        "jobs",
        "job",
        "all-jobs",
        "general",
        "vacancies",
        "careers",
        "all",
        "other",
    }
)

# Two-word titles that are clearly truncated fragments (e.g. "Head Of") — fold category in.
_ADZUNA_TITLE_FRAGMENT_SECONDS: frozenset[str] = frozenset({"of", "and", "the", "&"})


def adzuna_skill_blob_context(payload: dict[str, Any]) -> str:
    """Adzuna category tag + non-generic label for skill/title context (hyphens → spaces in tag)."""
    cat = payload.get("category")
    if not isinstance(cat, dict):
        return ""
    parts: list[str] = []
    tag = str(cat.get("tag") or "").strip().lower()
    if tag and tag not in _ADZUNA_GENERIC_CATEGORY_TAGS:
        parts.append(tag.replace("-", " "))
    lab = str(cat.get("label") or "").strip()
    if lab and lab.strip().lower() not in _ADZUNA_GENERIC_CATEGORY_LABELS:
        parts.append(lab)
    return _WS.sub(" ", " ".join(parts)).strip()


def adzuna_category_hint(payload: dict[str, Any]) -> str:
    """Same as adzuna_skill_blob_context (kept for callers that used the old name)."""
    return adzuna_skill_blob_context(payload)


def adzuna_title_norm_for_silver(title: str, payload: dict[str, Any]) -> str:
    """Normalize title; for very short Adzuna-only titles, append category tag for analytics (e.g. lead → lead - it-jobs)."""
    base = normalize_title_norm(title)
    if not base:
        return base
    words = base.split()
    if len(words) > 2:
        return base
    if len(words) == 2 and words[1] not in _ADZUNA_TITLE_FRAGMENT_SECONDS:
        return base
    cat = payload.get("category")
    if not isinstance(cat, dict):
        return base
    tag = str(cat.get("tag") or "").strip().lower()
    if not tag or tag in _ADZUNA_GENERIC_CATEGORY_TAGS:
        return base
    return f"{base} - {tag}"


def adzuna_location_for_silver(payload: dict[str, Any]) -> str:
    """Prefer city/state from `location.area` when `display_name` is country-only.

    Adzuna often sends `display_name: \"India\"` with `area: ['India']` only — keep that.
    When `display_name` is country-level but `area` has a full hierarchy, use city + state.
    """
    loc_obj = payload.get("location")
    if not isinstance(loc_obj, dict):
        return ""
    display = str(loc_obj.get("display_name") or "").strip()
    area_raw = loc_obj.get("area")
    area_list: list[str] = []
    if isinstance(area_raw, list):
        area_list = [str(a).strip() for a in area_raw if a is not None and str(a).strip()]
    al = [x.lower() for x in area_list]

    dlow = display.lower().strip()
    country_names = (
        "india",
        "in",
        "bharat",
        "united states",
        "usa",
        "united kingdom",
        "uk",
        "uae",
        "germany",
        "france",
    )
    is_country_only = dlow in country_names or (dlow in ("india", "in") and "," not in display)

    if is_country_only and len(area_list) >= 3:
        # Typical vendor order: country, region/state, city
        return f"{area_list[-1]}, {area_list[-2]}"
    if is_country_only and len(area_list) == 2:
        if al[0] in ("india", "in", "usa", "uk"):
            return area_list[-1]
        return f"{area_list[-1]}, {area_list[0]}"

    if display:
        return display
    if len(area_list) >= 3:
        return f"{area_list[-1]}, {area_list[-2]}"
    if len(area_list) == 2:
        return f"{area_list[-1]}, {area_list[0]}"
    if len(area_list) == 1:
        return area_list[0]
    return ""


_REMOTE_HYBRID_RE = re.compile(r"\bhybrid\b", re.IGNORECASE)
_REMOTE_REMOTE_RE = re.compile(
    r"\b(remote|wfh|work\s+from\s+home|work-from-home|work\s+remotely|fully\s+remote|remote\s+work|remote\s+role|telecommut(?:e|ing))\b",
    re.IGNORECASE,
)
_REMOTE_ONSITE_RE = re.compile(
    r"\b(onsite|on-site|on\s+site|office-based|office\s+based|report(?:ing)?\s+to\s+office|on-?prem(?:ises)?|work\s+from\s+office|office\s+only|based\s+in\s+office)\b",
    re.IGNORECASE,
)


def remote_type_from_adzuna_payload(payload: dict[str, Any], title: str, desc: str) -> str:
    """Adzuna has no boolean `remote` field — infer from title, description, category, and contract hints."""
    cat = payload.get("category")
    lab = ""
    tag = ""
    if isinstance(cat, dict):
        lab = str(cat.get("label") or "")
        tag = str(cat.get("tag") or "")
    ct = str(payload.get("contract_time") or "")
    cty = str(payload.get("contract_type") or "")
    text = f"{title}\n{desc}\n{lab}\n{tag}\n{ct}\n{cty}".strip().lower()
    if not text:
        return "unknown"
    if _REMOTE_HYBRID_RE.search(text):
        return "hybrid"
    if _REMOTE_REMOTE_RE.search(text):
        return "remote"
    if _REMOTE_ONSITE_RE.search(text):
        return "onsite"
    return "unknown"


def remote_type_for_silver(
    source: str,
    payload: dict[str, Any],
    *,
    title: str = "",
    description_plain: str = "",
) -> str:
    if source == "adzuna_in":
        return remote_type_from_adzuna_payload(payload, title, description_plain)
    return remote_type_from_arbeitnow_payload(payload)


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


def _skills_to_json_str(x: object) -> str:
    """Normalize any skills value to a JSON array string for flat Parquet storage."""
    import json as _json
    if x is None:
        return "[]"
    if isinstance(x, float) and pd.isna(x):
        return "[]"
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return "[]"
        if s.startswith("["):
            try:
                parsed = _json.loads(s)
                if isinstance(parsed, list):
                    return _json.dumps([str(t).strip() for t in parsed if t is not None and str(t).strip()])
            except (ValueError, TypeError):
                pass
        return _json.dumps([s])
    items: list = []
    if isinstance(x, (list, tuple)):
        items = list(x)
    elif hasattr(x, "tolist"):
        try:
            items = x.tolist()
        except Exception:
            return "[]"
    else:
        return "[]"
    cleaned = [str(t).strip() for t in items if t is not None and str(t).strip()]
    return _json.dumps(cleaned)


def skills_json_to_list(x: object) -> list[str]:
    """Deserialize skills JSON string back to a Python list (for Gold consumption)."""
    import json as _json
    if x is None:
        return []
    if isinstance(x, float) and pd.isna(x):
        return []
    if isinstance(x, str):
        s = x.strip()
        if not s or s == "[]":
            return []
        try:
            parsed = _json.loads(s)
            if isinstance(parsed, list):
                return [str(t).strip() for t in parsed if t is not None and str(t).strip()]
        except (ValueError, TypeError):
            pass
        return [s] if s else []
    if isinstance(x, (list, tuple)):
        return [str(t).strip() for t in x if t is not None and str(t).strip()]
    if hasattr(x, "tolist"):
        try:
            return [str(t).strip() for t in x.tolist() if t is not None and str(t).strip()]
        except Exception:
            return []
    return []


def project_silver_to_contract(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce exact Silver contract: only CANONICAL columns, fixed order (strips legacy/extra parquet fields)."""
    out = pd.DataFrame(index=df.index)
    for c in CANONICAL_SILVER_COLUMN_ORDER:
        if c in df.columns:
            out[c] = df[c]
        elif c == "skills":
            out[c] = "[]"
        else:
            out[c] = pd.NA
    out["skills"] = out["skills"].map(_skills_to_json_str)
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
