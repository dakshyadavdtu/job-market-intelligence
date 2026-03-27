from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

import requests

ARBEITNOW_URL = "https://www.arbeitnow.com/api/job-board-api"


def fetch_live_jobs(timeout_sec: int = 20) -> list[dict[str, Any]]:
    response = requests.get(ARBEITNOW_URL, timeout=timeout_sec)
    response.raise_for_status()
    payload = response.json()
    return payload.get("data", [])


def normalize_skill_tokens(raw_tags: list[str] | None) -> list[str]:
    if not raw_tags:
        return []
    return sorted({tag.strip().lower() for tag in raw_tags if tag and tag.strip()})


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
