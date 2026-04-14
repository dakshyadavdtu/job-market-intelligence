"""Shared incremental ingest contract: state prefix, job selection vs watermark, connector commit.

Both Arbeitnow and Adzuna use the same storage shape (`state/source=<slug>/connector_state.json`),
bootstrap via `load_connector_state`, client-side lookback when `incremental_strategy` is
`fallback_lookback`, and advance `fetch_watermark_created_at` from the **full API fetch** (not
only landed rows). Source-specific API calls stay in each ingest module.

Optional env ``JMI_BRONZE_INCREMENTAL_CUTOFF_CAP_TS`` (Unix seconds): for ``fallback_lookback`` with a
non-null watermark, the effective cutoff is ``min(wm - lookback_sec, cap_ts)``. **Lower** cap values
include **more** jobs from the current API response (still not invented—only rows the API returned).
Use e.g. ``0`` to land every job in the response with ``created_at > 0``, or set cap to just before
the earliest calendar month you need (e.g. 2026-03-01) so March/April postings are not dropped
when the default 48h lookback would exclude them.
"""

from __future__ import annotations

import os
from collections.abc import Callable

from src.jmi.config import AppConfig
from src.jmi.utils.io import ensure_dir
from src.jmi.utils.source_state import ConnectorState, connector_state_path, load_connector_state, save_connector_state


def _env_optional_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return None
    return int(str(raw).strip())


def ensure_connector_state_prefix(cfg: AppConfig) -> None:
    """Create parent dir for connector state (includes Arbeitnow slice subdirs when set)."""
    ensure_dir(connector_state_path(cfg).parent)


def load_incremental_connector_state(cfg: AppConfig) -> ConnectorState:
    """Bootstrap: ensure state prefix exists, then load JSON or default ConnectorState."""
    ensure_connector_state_prefix(cfg)
    return load_connector_state(cfg)


def select_jobs_for_bronze(
    cfg: AppConfig,
    state: ConnectorState,
    raw_jobs: list[dict],
    incremental_strategy: str,
    job_created_at_ts_fn: Callable[[dict], int],
) -> tuple[list[dict], dict]:
    """Return jobs to land in Bronze and diagnostic counters."""
    if not raw_jobs:
        raise RuntimeError("Source API returned no jobs; refusing to advance incremental state.")

    ts_fn = job_created_at_ts_fn
    ts_list = [ts_fn(j) for j in raw_jobs]
    max_ts = max(ts_list)
    min_ts = min(ts_list)
    lookback_sec = int(cfg.incremental_lookback_hours) * 3600
    wm = state.fetch_watermark_created_at

    if incremental_strategy == "true_api_filter":
        return raw_jobs, {
            "filter_mode": "true_api_filter",
            "api_job_count": len(raw_jobs),
            "landed_job_count": len(raw_jobs),
            "filtered_out_count": 0,
            "fetch_watermark_before": wm,
            "fetch_max_created_at_observed": max_ts,
            "fetch_min_created_at_observed": min_ts,
        }

    if wm is None:
        return raw_jobs, {
            "filter_mode": "fallback_lookback_bootstrap",
            "api_job_count": len(raw_jobs),
            "landed_job_count": len(raw_jobs),
            "filtered_out_count": 0,
            "fetch_watermark_before": None,
            "fetch_max_created_at_observed": max_ts,
            "fetch_min_created_at_observed": min_ts,
        }

    cutoff = wm - lookback_sec
    cap_ts = _env_optional_int("JMI_BRONZE_INCREMENTAL_CUTOFF_CAP_TS")
    if cap_ts is not None:
        cutoff = min(cutoff, cap_ts)
    selected = [j for j in raw_jobs if ts_fn(j) > cutoff]
    diag: dict = {
        "filter_mode": "fallback_lookback",
        "api_job_count": len(raw_jobs),
        "landed_job_count": len(selected),
        "filtered_out_count": len(raw_jobs) - len(selected),
        "fetch_watermark_before": wm,
        "fetch_cutoff_created_at_exclusive": cutoff,
        "fetch_max_created_at_observed": max_ts,
        "fetch_min_created_at_observed": min_ts,
    }
    if cap_ts is not None:
        diag["incremental_cutoff_cap_ts"] = cap_ts
    return selected, diag


def next_fetch_watermark_epoch(
    raw_jobs: list[dict],
    job_created_at_ts_fn: Callable[[dict], int],
) -> int:
    """Max `created` epoch over the full API response (watermark advances even if filter drops rows)."""
    if not raw_jobs:
        return 0
    return max(job_created_at_ts_fn(j) for j in raw_jobs)


def persist_incremental_connector_ok(
    cfg: AppConfig,
    *,
    run_id: str,
    batch_created_at: str,
    incremental_strategy: str,
    fetch_watermark_created_at: int,
) -> str:
    """Write successful-run connector state; returns path string."""
    updated = ConnectorState(
        source_name=cfg.source_name,
        last_successful_run_id=run_id,
        last_successful_run_at=batch_created_at,
        fetch_watermark_created_at=fetch_watermark_created_at,
        fallback_lookback_hours=cfg.incremental_lookback_hours,
        last_status="ok",
        incremental_strategy=incremental_strategy,
    )
    return save_connector_state(cfg, updated)
