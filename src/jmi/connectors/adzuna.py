"""
Adzuna Jobs API — India search (Phase 1: retrieval + schema inspection only).

Credentials (required for non-error responses):
  ADZUNA_APP_ID
  ADZUNA_APP_KEY

Optional: ADZUNA_ENV_FILE — path to a file with lines KEY=value (loads before connect).

Endpoint pattern (Adzuna REST):
  GET https://api.adzuna.com/v1/api/jobs/{country}/search/{page}

See: https://developer.adzuna.com/
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

import requests

ADZUNA_JOBS_API_ROOT = "https://api.adzuna.com/v1/api/jobs"
# Adzuna country code for India (confirmed by live 401 on /jobs/in/search/1 vs invalid path).
COUNTRY_INDIA = "in"
DEFAULT_TIMEOUT_SEC = 45
DEFAULT_RESULTS_PER_PAGE = 50
ADZUNA_SOURCE_SLUG = "adzuna_in"
ADZUNA_MAX_PAGES_DEFAULT = 3


def _hash_id(parts: list[str]) -> str:
    base = "|".join(p.strip().lower() for p in parts)
    return sha256(base.encode("utf-8")).hexdigest()


def job_created_at_ts(raw: dict[str, Any]) -> int:
    """Parse Adzuna `created` ISO 8601 (e.g. 2026-04-12T08:35:19Z) to Unix seconds for watermarks."""
    s = str(raw.get("created") or "").strip()
    if not s:
        return 0
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except (ValueError, TypeError, OSError, OverflowError):
        return 0


def build_stable_job_id(raw: dict[str, Any]) -> tuple[str, str]:
    """Primary key: Adzuna advertisement id; fallback: redirect_url; last resort: text hash."""
    sid = str(raw.get("id") or "").strip()
    if sid:
        return _hash_id(["adzuna_in", "id", sid]), "adzuna_id"

    url = str(raw.get("redirect_url") or "").strip()
    if url:
        return _hash_id(["adzuna_in", "redirect_url", url]), "adzuna_redirect_url"

    title = str(raw.get("title") or "")
    company = ""
    comp = raw.get("company")
    if isinstance(comp, dict):
        company = str(comp.get("display_name") or "")
    created = str(raw.get("created") or "")
    return _hash_id(["adzuna_in", "fallback", title, company, created]), "adzuna_fallback_text_time"


def to_bronze_record(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Thin Bronze envelope aligned with Arbeitnow: metadata + full vendor JSON in raw_payload.
    Adds source_job_id (Adzuna id) for Silver phase; source_slug mirrors id for parity with slug-based sources.
    """
    ingested_at = datetime.now(timezone.utc).isoformat()
    job_id, job_id_strategy = build_stable_job_id(raw)
    sid = str(raw.get("id") or "").strip()
    redirect = str(raw.get("redirect_url") or "").strip()

    return {
        "source": ADZUNA_SOURCE_SLUG,
        "schema_version": "v1",
        "job_id": job_id,
        "job_id_strategy": job_id_strategy,
        "source_job_id": sid,
        "source_slug": sid,
        "source_url": redirect,
        "ingested_at": ingested_at,
        "raw_payload": raw,
    }

# Minimal structure from Adzuna public docs (UK example); India uses the same job object shape.
_FIXTURE_JOB_SEARCH_RESULTS = {
    "__CLASS__": "Adzuna::API::Response::JobSearchResults",
    "results": [
        {
            "salary_min": 50000,
            "longitude": -0.776902,
            "location": {
                "__CLASS__": "Adzuna::API::Response::Location",
                "area": ["UK", "South East England", "Buckinghamshire", "Marlow"],
                "display_name": "Marlow, Buckinghamshire",
            },
            "salary_is_predicted": 0,
            "description": "JavaScript Developer Corporate ...",
            "__CLASS__": "Adzuna::API::Response::Job",
            "created": "2013-11-08T18:07:39Z",
            "latitude": 51.571999,
            "redirect_url": "http://adzuna.co.uk/jobs/land/ad/129698749",
            "title": "Javascript Developer",
            "category": {
                "__CLASS__": "Adzuna::API::Response::Category",
                "label": "IT Jobs",
                "tag": "it-jobs",
            },
            "id": "129698749",
            "salary_max": 55000,
            "company": {
                "__CLASS__": "Adzuna::API::Response::Company",
                "display_name": "Corporate Project Solutions",
            },
            "contract_type": "permanent",
        }
    ],
}


def _load_env_file(path: Path) -> None:
    """Minimal KEY=value loader (no export keyword). Skips if vars already set."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if key in ("ADZUNA_APP_ID", "ADZUNA_APP_KEY") and not (os.getenv(key) or "").strip():
            os.environ[key] = val


def _bootstrap_env() -> None:
    """Load optional ADZUNA_ENV_FILE or repo-root .env for local runs."""
    p = os.getenv("ADZUNA_ENV_FILE", "").strip()
    if p:
        _load_env_file(Path(p))
        return
    # Repo root: .../src/jmi/connectors/adzuna.py -> parents[3]
    root = Path(__file__).resolve().parents[3]
    _load_env_file(root / ".env")


def adzuna_credentials() -> tuple[str, str] | None:
    app_id = (os.getenv("ADZUNA_APP_ID") or "").strip()
    app_key = (os.getenv("ADZUNA_APP_KEY") or "").strip()
    if not app_id or not app_key:
        return None
    return app_id, app_key


def fetch_jobs_search(
    country: str,
    page: int = 1,
    *,
    results_per_page: int = DEFAULT_RESULTS_PER_PAGE,
    what: str | None = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    app_id: str | None = None,
    app_key: str | None = None,
) -> dict[str, Any]:
    """
    One Adzuna jobs search request. Returns the full JSON object (typically includes
    'results', 'count', and display fields — exact keys vary by version).
    """
    creds = adzuna_credentials()
    if app_id is None or app_key is None:
        if creds is None:
            raise OSError(
                "Missing ADZUNA_APP_ID and/or ADZUNA_APP_KEY in the environment."
            )
        app_id, app_key = creds

    url = f"{ADZUNA_JOBS_API_ROOT}/{country}/search/{page}"
    params: dict[str, Any] = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": results_per_page,
    }
    if what:
        params["what"] = what

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; JMI-job-market-intelligence/1.0)",
            "Accept": "application/json",
        }
    )
    response = session.get(url, params=params, timeout=timeout_sec)
    response.raise_for_status()
    return response.json()


def fetch_jobs_india_page1(
    *,
    results_per_page: int = DEFAULT_RESULTS_PER_PAGE,
    what: str | None = None,
) -> dict[str, Any]:
    """Convenience: first page of India job search."""
    return fetch_jobs_search(
        COUNTRY_INDIA,
        page=1,
        results_per_page=results_per_page,
        what=what,
    )


def fetch_all_jobs_india(
    *,
    max_pages: int | None = None,
    results_per_page: int = DEFAULT_RESULTS_PER_PAGE,
    what: str | None = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Paginate India search until a short page or max_pages.
    results_per_page capped at 50 per Adzuna typical limits.
    """
    cap = max(1, min(int(results_per_page), 50))
    pages_limit = max_pages
    if pages_limit is None:
        try:
            pages_limit = max(1, int(os.getenv("JMI_ADZUNA_MAX_PAGES", str(ADZUNA_MAX_PAGES_DEFAULT))))
        except ValueError:
            pages_limit = ADZUNA_MAX_PAGES_DEFAULT

    all_rows: list[dict[str, Any]] = []
    meta_last: dict[str, Any] = {}
    page = 1
    pages_done = 0
    while page <= pages_limit:
        if page > 1:
            time.sleep(0.35)
        payload = fetch_jobs_search(
            COUNTRY_INDIA,
            page,
            results_per_page=cap,
            what=what,
            timeout_sec=timeout_sec,
        )
        chunk = payload.get("results") or []
        if not isinstance(chunk, list):
            break
        meta_last = {
            "count": payload.get("count"),
            "mean": payload.get("mean"),
            "__CLASS__": payload.get("__CLASS__"),
        }
        all_rows.extend([j for j in chunk if isinstance(j, dict)])
        pages_done += 1
        if len(chunk) < cap:
            break
        page += 1

    return all_rows, {"meta": meta_last, "pages_fetched": pages_done, "results_per_page": cap}


def summarize_response(payload: dict[str, Any]) -> dict[str, Any]:
    """Lightweight structure summary for logging / CLI (no Bronze write)."""
    results = payload.get("results")
    n = len(results) if isinstance(results, list) else None
    first_keys: list[str] | None = None
    if isinstance(results, list) and results and isinstance(results[0], dict):
        first_keys = sorted(results[0].keys())
    return {
        "top_level_keys": sorted(payload.keys()),
        "results_len": n,
        "first_job_keys": first_keys,
        "count_field": payload.get("count"),
    }


def _value_kind(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, dict):
        return "object"
    if isinstance(v, list):
        return "array"
    return type(v).__name__


def inspect_payload(payload: dict[str, Any], *, sample_jobs: int = 15) -> dict[str, Any]:
    """
    Pagination, nullability across first N jobs, and suggested JMI mapping (informational).
    """
    results = payload.get("results")
    pagination = {
        "top_level_count": payload.get("count"),
        "top_level_mean": payload.get("mean"),
        "results_array_len": len(results) if isinstance(results, list) else None,
        "note": "Pagination: page number is in the URL path (.../search/{page}); "
        "total matches often in 'count' when present; use results_per_page query param.",
    }
    first_keys: list[str] = []
    null_stats: dict[str, dict[str, int]] = {}
    if isinstance(results, list) and results:
        for j in results[:sample_jobs]:
            if not isinstance(j, dict):
                continue
            if not first_keys:
                first_keys = sorted(j.keys())
            for k, v in j.items():
                st = null_stats.setdefault(k, {"present": 0, "null": 0, "empty_str": 0})
                if v is None:
                    st["null"] += 1
                elif isinstance(v, str) and not v.strip():
                    st["empty_str"] += 1
                else:
                    st["present"] += 1

    kinds: dict[str, str] = {}
    if isinstance(results, list) and results and isinstance(results[0], dict):
        for k, v in results[0].items():
            kinds[k] = _value_kind(v)

    mapping = {
        "jmi_bronze_job_id": "hash(['adzuna_in', str(job['id'])]) or hash(redirect_url)",
        "jmi_raw_title": "job['title']",
        "jmi_raw_company": "job['company']['display_name'] if company is dict else company",
        "jmi_raw_location": "job['location']['display_name'] or join area[]",
        "jmi_category": "job['category']['label'] / ['tag']",
        "jmi_posted_at": "job['created'] (ISO string; NOT Unix like Arbeitnow)",
        "jmi_salary_min_max": "job['salary_min'], job['salary_max'] (may be 0 or missing)",
        "jmi_apply_url": "job['redirect_url']",
        "jmi_description": "job['description'] (snippet only per Adzuna docs)",
        "jmi_skills": "no tags array — use extract_silver_skills(None, title, description)",
    }

    return {
        "pagination": pagination,
        "first_job_keys_sorted": first_keys,
        "first_job_value_kinds": kinds,
        "nullability_first_n_jobs": null_stats,
        "jmi_field_mapping_hints": mapping,
    }


def _main() -> int:
    _bootstrap_env()
    parser = argparse.ArgumentParser(description="Adzuna India jobs: fetch + inspect (Phase 1).")
    parser.add_argument(
        "--fixture",
        action="store_true",
        help="Use embedded doc-example JSON (no API call; for structure tests).",
    )
    parser.add_argument(
        "--results-per-page",
        type=int,
        default=min(20, DEFAULT_RESULTS_PER_PAGE),
        help="results_per_page for live India search",
    )
    parser.add_argument(
        "--dump-json",
        metavar="PATH",
        help="Write full JSON response to PATH (live or fixture).",
    )
    args = parser.parse_args()

    if args.fixture:
        payload = dict(_FIXTURE_JOB_SEARCH_RESULTS)
    else:
        creds = adzuna_credentials()
        if creds is None:
            print(
                "Missing credentials: set ADZUNA_APP_ID and ADZUNA_APP_KEY "
                "(optional: repo-root .env or ADZUNA_ENV_FILE).",
                file=sys.stderr,
            )
            return 2
        payload = fetch_jobs_india_page1(results_per_page=max(1, min(args.results_per_page, 50)))

    if args.dump_json:
        Path(args.dump_json).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    out = {
        "summary": summarize_response(payload),
        "inspect": inspect_payload(payload),
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    print("--- first job (stderr, truncated) ---", file=sys.stderr)
    results = payload.get("results")
    if isinstance(results, list) and results:
        sample = json.dumps(results[0], indent=2, ensure_ascii=False)
        print(sample[:6000] + ("..." if len(sample) > 6000 else ""), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
