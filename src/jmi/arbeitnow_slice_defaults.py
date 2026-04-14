"""Defaults for Arbeitnow focused slice (2026 Q1 + any 2025 rows still listed). Not used unless JMI_ARBEITNOW_SLICE is set."""

from __future__ import annotations

# Unix seconds 2025-01-01 00:00:00 UTC — API min_created_at floor to include still-listed 2025 rows
# while targeting 2026-01 / 2026-02 / 2026-03 (posting months from created_at).
ARBEITNOW_SLICE_DEFAULT_MIN_CREATED_AT_UTC = 1735689600

# Recommended slice tag for isolated Bronze/Silver/Gold paths (override with JMI_ARBEITNOW_SLICE).
ARBEITNOW_SLICE_TAG_DEFAULT = "arbeitnow_2026_q1_focus"

# Optional env (see arbeitnow.fetch_all_jobs): JMI_ARBEITNOW_MAX_PAGES, JMI_ARBEITNOW_PAGE_SLEEP_SEC,
# JMI_ARBEITNOW_REQUEST_TIMEOUT_SEC — run_arbeitnow_2026_q1_slice.sh sets higher defaults for max capture.
