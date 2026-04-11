from __future__ import annotations

import time
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

import requests

ARBEITNOW_URL = "https://www.arbeitnow.com/api/job-board-api"
ARBEITNOW_MAX_PAGES = 2000


# Keep MVP skill output practical with a small rule-based vocabulary.
SKILL_ALIAS_MAP: dict[str, list[str]] = {
    "sap/erp consulting": ["sap", "erp"],
    "system and network administration": ["network administration", "systems administration"],
    "online marketing": ["digital marketing"],
    "recruitment and selection": ["recruiting"],
    "data engineer": ["data engineering"],
    "automation engineering": ["automation"],
}

SKILL_ALLOWLIST: set[str] = {
    "automation",
    "compliance",
    "data engineering",
    "data processing",
    "digital marketing",
    "erp",
    "information systems",
    "network administration",
    "recruiting",
    "sap",
    "security",
    "systems administration",
}

SKILL_STOPLIST: set[str] = {
    "accounts receivable",
    "administration",
    "asset",
    "building",
    "chief executives",
    "consulting",
    "controlling",
    "development",
    "directors",
    "engineering",
    "finance",
    "fonds management",
    "hr",
    "it",
    "management",
    "marketing and communication",
    "marketing manager",
    "private banking",
    "process management",
    "product management",
    "project management",
    "remote",
    "safety services engineering",
    "software development",
    "supply",
    "team leader",
}


def job_created_at_ts(raw: dict[str, Any]) -> int:
    """Arbeitnow uses Unix epoch seconds on created_at."""
    v = raw.get("created_at")
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip()
    if not s:
        return 0
    if s.isdigit():
        return int(s)
    return 0


def fetch_live_jobs(timeout_sec: int = 20) -> list[dict[str, Any]]:
    """Backward-compatible single-call fetch (first page only). Prefer fetch_all_jobs for ingestion."""
    response = requests.get(ARBEITNOW_URL, timeout=timeout_sec)
    response.raise_for_status()
    payload = response.json()
    return payload.get("data", [])


def fetch_all_jobs(
    timeout_sec: int = 45,
    min_created_at: int | None = None,
    use_min_created_at_param: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Paginate the public job-board API until a short page is returned.
    If use_min_created_at_param and min_created_at are set, adds min_created_at to the query (Case A when supported).
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; JMI-job-market-intelligence/1.0; +https://github.com/)",
            "Accept": "application/json",
        }
    )
    all_rows: list[dict[str, Any]] = []
    page = 1
    meta_last: dict[str, Any] = {}
    while page <= ARBEITNOW_MAX_PAGES:
        params: dict[str, Any] = {"page": page}
        if use_min_created_at_param and min_created_at is not None:
            params["min_created_at"] = min_created_at
        response: requests.Response | None = None
        for attempt in range(4):
            if page > 1:
                time.sleep(0.35)
            response = session.get(ARBEITNOW_URL, params=params, timeout=timeout_sec)
            if response.status_code in (403, 429) and attempt < 3:
                time.sleep(1.5 * (2**attempt))
                continue
            response.raise_for_status()
            break
        if response is None:
            raise RuntimeError("fetch_all_jobs: failed to obtain response")
        payload = response.json()
        chunk = payload.get("data") or []
        meta_last = payload.get("meta") or meta_last
        all_rows.extend(chunk)
        per_page = int(meta_last.get("per_page") or 100)
        if len(chunk) < per_page:
            break
        page += 1
    return all_rows, {"meta": meta_last, "pages_fetched": page}


def normalize_skill_tokens(raw_tags: list[str] | None) -> list[str]:
    if not raw_tags:
        return []
    skills: set[str] = set()
    for tag in raw_tags:
        token = str(tag or "").strip().lower()
        if not token or token in SKILL_STOPLIST:
            continue
        expanded = SKILL_ALIAS_MAP.get(token, [token])
        for candidate in expanded:
            if candidate in SKILL_ALLOWLIST:
                skills.add(candidate)
    return sorted(skills)


def _hash_id(parts: list[str]) -> str:
    base = "|".join(p.strip().lower() for p in parts)
    return sha256(base.encode("utf-8")).hexdigest()


def build_stable_job_id(raw: dict[str, Any]) -> tuple[str, str]:
    """
    Deterministic id strategy:
    1) source slug
    2) canonical URL
    3) fallback hash on stable text/time fields
    """
    slug = str(raw.get("slug") or "").strip()
    if slug:
        return _hash_id(["arbeitnow", "slug", slug]), "slug"

    url = str(raw.get("url") or "").strip()
    if url:
        return _hash_id(["arbeitnow", "url", url]), "url"

    title = str(raw.get("title") or "")
    company = str(raw.get("company_name") or "")
    location = str(raw.get("location") or "")
    published_at = str(raw.get("created_at") or "")
    return _hash_id(["arbeitnow", "fallback", title, company, location, published_at]), "fallback_text_time"


def to_bronze_record(raw: dict[str, Any]) -> dict[str, Any]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    job_id, job_id_strategy = build_stable_job_id(raw)

    return {
        "source": "arbeitnow",
        "schema_version": "v1",
        "job_id": job_id,
        "job_id_strategy": job_id_strategy,
        "source_slug": str(raw.get("slug") or ""),
        "source_url": str(raw.get("url") or ""),
        "ingested_at": ingested_at,
        "raw_payload": raw,
    }
