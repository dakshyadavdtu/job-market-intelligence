"""March strict layer: materialize only if strict intersection contains a *-03 posted_month for both sources."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd

from src.jmi.config import AppConfig
from src.jmi.paths import (
    derived_march_strict_benchmark_summary_parquet,
    derived_march_strict_manifest_parquet,
    derived_march_strict_month_totals_parquet,
    derived_march_strict_role_mix_parquet,
    derived_march_strict_skill_mix_parquet,
)
from src.jmi.pipelines.transform_derived_strict_common import (
    ADZUNA_IN,
    ARBEITNOW,
    _latest_run_id,
    _load_fact_with_posted_month,
    _months_with_role_partition_for_run,
    _read_parquet_any,
)
from src.jmi.utils.io import ensure_dir, write_parquet

LAYER = "march_strict_intersection"
LAYER_STRICT = "strict_common_month"


def run_derived_march_strict(cfg: AppConfig | None = None) -> dict:
    cfg = cfg or AppConfig()
    rid_an = _latest_run_id(cfg, ARBEITNOW)
    rid_ad = _latest_run_id(cfg, ADZUNA_IN)
    m_an = _months_with_role_partition_for_run(cfg, ARBEITNOW, rid_an) if rid_an else set()
    m_ad = _months_with_role_partition_for_run(cfg, ADZUNA_IN, rid_ad) if rid_ad else set()
    strict_months_sorted = sorted(m_an & m_ad)
    march_months_sorted = sorted(m for m in strict_months_sorted if str(m).endswith("-03"))

    replacement = strict_months_sorted[-1] if strict_months_sorted else ""

    if not march_months_sorted:
        return {
            "status": "REJECTED",
            "march_materialized": False,
            "s3_written": False,
            "rejection_reason": (
                "Strict intersection of latest Gold role_demand months contains no posted_month ending in -03 "
                "for both arbeitnow and adzuna_in; March-only comparison is not materialized."
            ),
            "strict_intersection_months": strict_months_sorted,
            "strict_comparable_replacement_month": replacement,
            "note": "Use derived/comparison/strict_common_month/ for the honest shared comparable month(s).",
        }

    march_set = set(march_months_sorted)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    manifest = pd.DataFrame(
        [
            {
                "layer_scope": LAYER,
                "run_id_arbeitnow": rid_an or "",
                "run_id_adzuna_in": rid_ad or "",
                "march_posted_months_csv": ",".join(march_months_sorted),
                "march_month_count": len(march_months_sorted),
                "strict_intersection_superset_csv": ",".join(strict_months_sorted),
                "materialized_at_utc": ts,
            }
        ]
    )

    total_rows: list[dict] = []
    for src, rid in ((ARBEITNOW, rid_an), (ADZUNA_IN, rid_ad)):
        if not rid:
            continue
        for pm in march_months_sorted:
            if pm not in march_set:
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

    bench = pd.DataFrame(
        [
            {
                "layer_scope": LAYER,
                "march_strict_latest_month": march_months_sorted[-1],
                "march_month_count": len(march_months_sorted),
                "run_id_arbeitnow": rid_an or "",
                "run_id_adzuna_in": rid_ad or "",
                "materialized_at_utc": ts,
            }
        ]
    )

    skill_frames: list[pd.DataFrame] = []
    role_frames: list[pd.DataFrame] = []
    for src, rid in ((ARBEITNOW, rid_an), (ADZUNA_IN, rid_ad)):
        if not rid:
            continue
        s_df = _load_fact_with_posted_month(cfg, "skill_demand_monthly", src, rid, march_set)
        if not s_df.empty:
            s_df = s_df.copy()
            s_df["source"] = src
            s_df["run_id"] = rid
            s_df["layer_scope"] = LAYER
            skill_frames.append(s_df)
        r_df = _load_fact_with_posted_month(cfg, "role_demand_monthly", src, rid, march_set)
        if not r_df.empty:
            r_df = r_df.copy()
            r_df["source"] = src
            r_df["run_id"] = rid
            r_df["layer_scope"] = LAYER
            role_frames.append(r_df)

    skill_out = pd.concat(skill_frames, ignore_index=True) if skill_frames else pd.DataFrame()
    role_out = pd.concat(role_frames, ignore_index=True) if role_frames else pd.DataFrame()

    paths_written: dict[str, str] = {}
    for label, df, dest_fn in (
        ("manifest", manifest, derived_march_strict_manifest_parquet),
        ("month_totals", month_totals, derived_march_strict_month_totals_parquet),
        ("benchmark_summary", bench, derived_march_strict_benchmark_summary_parquet),
        ("skill_mix", skill_out, derived_march_strict_skill_mix_parquet),
        ("role_mix", role_out, derived_march_strict_role_mix_parquet),
    ):
        path = dest_fn(cfg)
        ensure_dir(path.parent)
        write_parquet(path, df)
        paths_written[label] = str(path)

    meta = cfg.quality_root / "derived_march_strict.json"
    ensure_dir(meta.parent)
    meta.write_text(
        json.dumps(
            {
                "stage": "derived_march_strict",
                "march_posted_months": march_months_sorted,
                "paths": paths_written,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "status": "OK",
        "march_materialized": True,
        "s3_written": True,
        "march_posted_months": march_months_sorted,
        "strict_intersection_months": strict_months_sorted,
        "paths": paths_written,
        "reference_strict_layer": LAYER_STRICT,
    }


if __name__ == "__main__":
    print(json.dumps(run_derived_march_strict(), indent=2))
