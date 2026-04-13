#!/usr/bin/env python3
"""Print recommended env vars to land honest multi-month API rows and check presentation months."""
from __future__ import annotations

# Epoch for 2026-03-01 00:00:00 UTC (for Arbeitnow min_created_at when using API filter).
MARCH_2026_UTC = 1772323200

print(
    """
# 1) Widen incremental Bronze landing (API rows only — not invented):
#    Land all jobs in each API response with created_at > 0 (Adzuna + Arbeitnow fallback_lookback).
export JMI_BRONZE_INCREMENTAL_CUTOFF_CAP_TS=0

# 2) Or: cap cutoff to just before March 2026 so March+April postings are not dropped by 48h lookback:
# export JMI_BRONZE_INCREMENTAL_CUTOFF_CAP_TS=%s

# 3) Longer lookback window (hours) — combines with watermark:
export JMI_INCREMENTAL_LOOKBACK_HOURS=2160

# 4) Arbeitnow: optional API-side min_created_at (epoch seconds) to page from March 2026 onward:
export JMI_ARBEITNOW_USE_MIN_CREATED_AT_PARAM=true
export JMI_ARBEITNOW_MIN_CREATED_AT=%s

# 5) Adzuna: more search pages (honest API pagination; default in code is now higher):
# export JMI_ADZUNA_MAX_PAGES=40

# 6) After Bronze/Silver/Gold rebuild, presentation manifest checks target months:
export JMI_PRESENTATION_REQUIRED_MONTHS=2026-03,2026-04

# 7) Repair merged/latest from full silver batch union (optional):
# python scripts/rebuild_merged_silver_from_union.py --source arbeitnow
# python scripts/rebuild_merged_silver_from_union.py --source adzuna_in
"""
    % (MARCH_2026_UTC - 1, MARCH_2026_UTC)
)
