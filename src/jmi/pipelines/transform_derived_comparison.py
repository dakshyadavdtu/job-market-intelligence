"""Silver merged → cross-source comparison totals (posted month × source). Not a third Gold source."""

from __future__ import annotations

import json

import pandas as pd

from src.jmi.config import AppConfig
from src.jmi.paths import derived_comparison_totals_parquet
from src.jmi.pipelines.gold_time import assign_posted_month_and_time_axis, dominant_time_axis
from src.jmi.utils.io import ensure_dir, write_parquet


def run_derived_comparison(cfg: AppConfig | None = None) -> dict:
    """Build `derived/comparison/posted_month_source_totals` from each source's merged Silver."""
    cfg = cfg or AppConfig()
    rows: list[dict] = []
    for src in ("arbeitnow", "adzuna_in"):
        p = cfg.silver_root.as_path() / "jobs" / f"source={src}" / "merged" / "latest.parquet"
        if not p.is_file():
            continue
        df = pd.read_parquet(p)
        if df.empty:
            continue
        df = assign_posted_month_and_time_axis(df)
        df = df[df["posted_month"].astype(str).str.match(r"^\d{4}-\d{2}$", na=False)]
        for pm, g in df.groupby("posted_month"):
            rows.append(
                {
                    "source": src,
                    "posted_month": str(pm),
                    "job_count": int(len(g)),
                    "time_axis": dominant_time_axis(g["time_axis"]) if "time_axis" in g.columns else "posted",
                }
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return {"status": "EMPTY", "message": "No merged Silver files found for comparison.", "row_count": 0}

    dest = derived_comparison_totals_parquet(cfg)
    write_parquet(dest, out.sort_values(["posted_month", "source"]))
    meta = cfg.quality_root / "derived_comparison_totals.json"
    ensure_dir(meta.parent)
    meta.write_text(
        json.dumps(
            {
                "stage": "derived_comparison",
                "output_file": str(dest),
                "row_count": int(len(out)),
                "sources_present": sorted(out["source"].unique().tolist()),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return {"status": "OK", "output_file": str(dest), "row_count": int(len(out))}


if __name__ == "__main__":
    print(json.dumps(run_derived_comparison(), indent=2))
