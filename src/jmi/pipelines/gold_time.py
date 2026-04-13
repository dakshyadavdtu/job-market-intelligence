"""Gold time semantics: analysis month = when the job was posted (Silver `posted_at`), not pipeline ingest date."""

from __future__ import annotations

import warnings

import pandas as pd


def _posted_at_to_utc(s: pd.Series) -> pd.Series:
    """Parse Silver `posted_at`: ISO-8601, datetimes, and Unix epoch seconds (Arbeitnow sometimes lands epoch as string)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        ts = pd.to_datetime(s, utc=True, errors="coerce", format="mixed")
    strv = s.astype(str)
    epoch_str = ts.isna() & strv.str.fullmatch(r"\d{10,12}", na=False)
    if epoch_str.any():
        nums = pd.to_numeric(strv[epoch_str], errors="coerce")
        ts = ts.copy()
        ts.loc[epoch_str] = pd.to_datetime(nums, unit="s", utc=True, errors="coerce")
    num = pd.to_numeric(s, errors="coerce")
    epoch_num = ts.isna() & num.notna() & (num >= 1e9) & (num < 1e11)
    if epoch_num.any():
        ts = ts.copy()
        ts.loc[epoch_num] = pd.to_datetime(num[epoch_num], unit="s", utc=True, errors="coerce")
    return ts


def assign_posted_month_and_time_axis(df: pd.DataFrame) -> pd.DataFrame:
    """Add `posted_month` (YYYY-MM) and `time_axis` ('posted' | 'ingest_fallback').

    Primary: first calendar month of `posted_at` (ISO). Fallback: month of `bronze_ingest_date`
    when `posted_at` is missing or unparseable — honest for analytics with lineage.
    """
    out = df.copy()
    if "posted_at" not in out.columns:
        out["posted_at"] = pd.NA
    ts = _posted_at_to_utc(out["posted_at"])
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
