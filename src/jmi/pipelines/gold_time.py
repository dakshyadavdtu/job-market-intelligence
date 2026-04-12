"""Gold time semantics: analysis month = when the job was posted (Silver `posted_at`), not pipeline ingest date."""

from __future__ import annotations

import warnings

import pandas as pd


def assign_posted_month_and_time_axis(df: pd.DataFrame) -> pd.DataFrame:
    """Add `posted_month` (YYYY-MM) and `time_axis` ('posted' | 'ingest_fallback').

    Primary: first calendar month of `posted_at` (ISO). Fallback: month of `bronze_ingest_date`
    when `posted_at` is missing or unparseable — honest for analytics with lineage.
    """
    out = df.copy()
    if "posted_at" not in out.columns:
        out["posted_at"] = pd.NA
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        ts = pd.to_datetime(out["posted_at"], utc=True, errors="coerce", format="mixed")
    out["posted_month"] = ts.dt.strftime("%Y-%m")
    bad = pd.isna(out["posted_month"]) | (out["posted_month"].astype(str) == "NaT")
    if "bronze_ingest_date" not in out.columns:
        out["bronze_ingest_date"] = ""
    ingest_m = out["bronze_ingest_date"].astype(str).str[:7]
    out.loc[bad, "posted_month"] = ingest_m.loc[bad]
    out["time_axis"] = "posted"
    out.loc[bad, "time_axis"] = "ingest_fallback"
    # Normalize invalid YYYY-MM from bad bronze dates
    pm = out["posted_month"].astype(str)
    valid = pm.str.match(r"^\d{4}-\d{2}$", na=False)
    out.loc[~valid, "posted_month"] = ""
    out.loc[~valid, "time_axis"] = "ingest_fallback"
    return out


def dominant_time_axis(series: pd.Series) -> str:
    u = [x for x in series.dropna().unique() if str(x).strip()]
    if not u:
        return "ingest_fallback"
    if len(u) == 1:
        return str(u[0])
    return "mixed"
