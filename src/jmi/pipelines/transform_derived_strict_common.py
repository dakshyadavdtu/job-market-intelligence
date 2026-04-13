"""Materialize strict common posted_month layer from Gold latest runs (intersection EU ∩ India)."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone

import pandas as pd

from src.jmi.config import AppConfig, split_s3_uri
from src.jmi.paths import (
    derived_strict_common_benchmark_summary_parquet,
    derived_strict_common_manifest_parquet,
    derived_strict_common_month_totals_parquet,
    derived_strict_common_role_mix_parquet,
    derived_strict_common_skill_mix_parquet,
)
from src.jmi.utils.io import ensure_dir, write_parquet

ARBEITNOW = "arbeitnow"
ADZUNA_IN = "adzuna_in"
LAYER = "strict_common_month"


def _read_parquet_any(path: object) -> pd.DataFrame | None:
    try:
        df = pd.read_parquet(str(path))
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None


def _latest_run_id(cfg: AppConfig, source: str) -> str | None:
    p = cfg.gold_root / f"source={source}" / "latest_run_metadata" / "part-00001.parquet"
    df = _read_parquet_any(p)
    if df is None or "run_id" not in df.columns:
        return None
    return str(df["run_id"].iloc[0]).strip()


def _months_with_role_partition_for_run(cfg: AppConfig, source: str, run_id: str) -> set[str]:
    """Distinct posted_month for which role_demand_monthly exists for this latest run_id (matches Athena views)."""
    months: set[str] = set()
    if cfg.gold_root.is_s3:
        bucket, base_prefix = split_s3_uri(str(cfg.gold_root).rstrip("/") + "/")
        prefix = f"{base_prefix}role_demand_monthly/source={source}/"
        rx = re.compile(r"posted_month=([^/]+)/run_id=" + re.escape(run_id) + r"/part-00001\.parquet$")
        import boto3  # type: ignore

        cli = boto3.client("s3")
        paginator = cli.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents") or []:
                m = rx.search(obj["Key"])
                if m:
                    months.add(m.group(1))
        return months

    base = cfg.gold_root.as_path() / "role_demand_monthly" / f"source={source}"
    if not base.is_dir():
        return months
    for d in base.iterdir():
        if not d.is_dir() or not d.name.startswith("posted_month="):
            continue
        pm = d.name.split("=", 1)[1]
        part = d / f"run_id={run_id}" / "part-00001.parquet"
        if part.is_file():
            months.add(pm)
    return months


def _load_fact_with_posted_month(
    cfg: AppConfig,
    table: str,
    source: str,
    run_id: str,
    months: set[str],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for pm in sorted(months):
        p = cfg.gold_root / table / f"source={source}" / f"posted_month={pm}" / f"run_id={run_id}" / "part-00001.parquet"
        df = _read_parquet_any(p)
        if df is None:
            continue
        df = df.copy()
        df["posted_month"] = pm
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def run_derived_strict_common(cfg: AppConfig | None = None) -> dict:
    cfg = cfg or AppConfig()
    rid_an = _latest_run_id(cfg, ARBEITNOW)
    rid_ad = _latest_run_id(cfg, ADZUNA_IN)
    m_an = _months_with_role_partition_for_run(cfg, ARBEITNOW, rid_an) if rid_an else set()
    m_ad = _months_with_role_partition_for_run(cfg, ADZUNA_IN, rid_ad) if rid_ad else set()
    strict_months_sorted = sorted(m_an & m_ad)
    strict_set = set(strict_months_sorted)

    manifest = pd.DataFrame(
        [
            {
                "layer_scope": LAYER,
                "run_id_arbeitnow": rid_an or "",
                "run_id_adzuna_in": rid_ad or "",
                "strict_months_csv": ",".join(strict_months_sorted),
                "strict_intersection_month_count": len(strict_months_sorted),
                "strict_intersection_latest_month": strict_months_sorted[-1] if strict_months_sorted else "",
                "march_in_strict_intersection": any(m.endswith("-03") for m in strict_months_sorted),
                "materialized_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        ]
    )

    total_rows: list[dict] = []
    for src, rid in ((ARBEITNOW, rid_an), (ADZUNA_IN, rid_ad)):
        if not rid or not strict_set:
            continue
        for pm in strict_months_sorted:
            if pm not in strict_set:
                continue
            p = cfg.gold_root / "role_demand_monthly" / f"source={src}" / f"posted_month={pm}" / f"run_id={rid}" / "part-00001.parquet"
            df = _read_parquet_any(p)
            if df is None:
                continue
            total_postings = int(df["job_count"].sum()) if "job_count" in df.columns else 0
            total_rows.append(
                {
                    "source": src,
                    "posted_month": pm,
                    "run_id": rid,
                    "total_postings": total_postings,
                    "layer_scope": LAYER,
                }
            )

    month_totals = pd.DataFrame(total_rows)

    an_march = bool(m_an and any(x.endswith("-03") for x in m_an))
    ad_march = bool(m_ad and any(x.endswith("-03") for x in m_ad))
    bench = pd.DataFrame(
        [
            {
                "layer_scope": LAYER,
                "strict_intersection_latest_month": strict_months_sorted[-1] if strict_months_sorted else "",
                "strict_intersection_month_count": len(strict_months_sorted),
                "march_strict_comparable_both_sources": an_march and ad_march,
                "run_id_arbeitnow": rid_an or "",
                "run_id_adzuna_in": rid_ad or "",
                "materialized_at_utc": manifest["materialized_at_utc"].iloc[0],
            }
        ]
    )

    skill_frames: list[pd.DataFrame] = []
    role_frames: list[pd.DataFrame] = []
    for src, rid in ((ARBEITNOW, rid_an), (ADZUNA_IN, rid_ad)):
        if not rid or not strict_set:
            continue
        s_df = _load_fact_with_posted_month(cfg, "skill_demand_monthly", src, rid, strict_set)
        if not s_df.empty:
            s_df = s_df.copy()
            s_df["source"] = src
            s_df["run_id"] = rid
            s_df["layer_scope"] = LAYER
            skill_frames.append(s_df)
        r_df = _load_fact_with_posted_month(cfg, "role_demand_monthly", src, rid, strict_set)
        if not r_df.empty:
            r_df = r_df.copy()
            r_df["source"] = src
            r_df["run_id"] = rid
            r_df["layer_scope"] = LAYER
            role_frames.append(r_df)

    skill_out = pd.concat(skill_frames, ignore_index=True) if skill_frames else pd.DataFrame()
    role_out = pd.concat(role_frames, ignore_index=True) if role_frames else pd.DataFrame()

    paths_written: dict[str, str] = {}
    out_manifest = derived_strict_common_manifest_parquet(cfg)
    ensure_dir(out_manifest.parent)
    write_parquet(out_manifest, manifest)
    paths_written["manifest"] = str(out_manifest)

    out_mt = derived_strict_common_month_totals_parquet(cfg)
    ensure_dir(out_mt.parent)
    write_parquet(out_mt, month_totals)
    paths_written["month_totals"] = str(out_mt)

    out_b = derived_strict_common_benchmark_summary_parquet(cfg)
    ensure_dir(out_b.parent)
    write_parquet(out_b, bench)
    paths_written["benchmark_summary"] = str(out_b)

    out_s = derived_strict_common_skill_mix_parquet(cfg)
    ensure_dir(out_s.parent)
    write_parquet(out_s, skill_out)
    paths_written["skill_mix"] = str(out_s)

    out_r = derived_strict_common_role_mix_parquet(cfg)
    ensure_dir(out_r.parent)
    write_parquet(out_r, role_out)
    paths_written["role_mix"] = str(out_r)

    meta = cfg.quality_root / "derived_strict_common.json"
    ensure_dir(meta.parent)
    meta.write_text(
        json.dumps(
            {
                "stage": "derived_strict_common_month",
                "strict_months": strict_months_sorted,
                "paths": paths_written,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "status": "OK",
        "strict_months": strict_months_sorted,
        "row_counts": {
            "manifest": int(len(manifest)),
            "month_totals": int(len(month_totals)),
            "benchmark_summary": int(len(bench)),
            "skill_mix": int(len(skill_out)),
            "role_mix": int(len(role_out)),
        },
        "paths": paths_written,
    }


if __name__ == "__main__":
    print(json.dumps(run_derived_strict_common(), indent=2))
