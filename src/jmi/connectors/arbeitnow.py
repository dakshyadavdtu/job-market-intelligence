from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

import requests

ARBEITNOW_URL = "https://www.arbeitnow.com/api/job-board-api"

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


def fetch_live_jobs(timeout_sec: int = 20) -> list[dict[str, Any]]:
    response = requests.get(ARBEITNOW_URL, timeout=timeout_sec)
    response.raise_for_status()
    payload = response.json()
    return payload.get("data", [])


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
