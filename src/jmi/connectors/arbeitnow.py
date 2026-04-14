from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

import requests

from src.jmi.connectors.skill_extract import extract_silver_skills

ARBEITNOW_URL = "https://www.arbeitnow.com/api/job-board-api"
ARBEITNOW_MAX_PAGES_DEFAULT = 2000


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return int(str(raw).strip())


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return float(str(raw).strip())


def arbeitnow_max_pages() -> int:
    """Upper bound on paginated fetches; override with JMI_ARBEITNOW_MAX_PAGES (higher = more rows if API has more pages)."""
    return max(1, _env_int("JMI_ARBEITNOW_MAX_PAGES", ARBEITNOW_MAX_PAGES_DEFAULT))


def normalize_skill_tokens(raw_tags: list[str] | None) -> list[str]:
    """Backward-compatible: tags-only extraction (title/description empty). Prefer extract_silver_skills in Silver."""
    return extract_silver_skills(raw_tags, "", "")


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
    timeout_sec: int | None = None,
    min_created_at: int | None = None,
    use_min_created_at_param: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Paginate the public job-board API until a short page is returned.
    If use_min_created_at_param and min_created_at are set, adds min_created_at to the query (Case A when supported).

    Env (optional): JMI_ARBEITNOW_MAX_PAGES, JMI_ARBEITNOW_PAGE_SLEEP_SEC, JMI_ARBEITNOW_REQUEST_TIMEOUT_SEC.
    """
    max_pages = arbeitnow_max_pages()
    if timeout_sec is None:
        timeout_sec = max(15, _env_int("JMI_ARBEITNOW_REQUEST_TIMEOUT_SEC", 45))
    sleep_between_pages = max(0.0, _env_float("JMI_ARBEITNOW_PAGE_SLEEP_SEC", 0.35))
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
    pages_fetched = 0
    while page <= max_pages:
        params: dict[str, Any] = {"page": page}
        if use_min_created_at_param and min_created_at is not None:
            params["min_created_at"] = min_created_at
        response: requests.Response | None = None
        for attempt in range(6):
            if page > 1:
                time.sleep(sleep_between_pages)
            response = session.get(ARBEITNOW_URL, params=params, timeout=timeout_sec)
            code = response.status_code
            if code in (403, 429) or (500 <= code < 600):
                if attempt < 5:
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
        pages_fetched += 1
        per_page = int(meta_last.get("per_page") or 100)
        if len(chunk) < per_page:
            break
        page += 1
    return all_rows, {
        "meta": meta_last,
        "pages_fetched": pages_fetched,
        "max_pages_cap": max_pages,
        "total_rows": len(all_rows),
        "timeout_sec": timeout_sec,
        "page_sleep_sec": sleep_between_pages,
    }


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
