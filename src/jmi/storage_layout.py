"""Active lake layout enforcement (local disk): remove legacy prefixes before S3 sync.

Canonical active layout is defined in ``src.jmi.paths``. Legacy objects must not be re-uploaded
to active ``s3://.../gold/`` or ``s3://.../silver/`` prefixes.
"""

from __future__ import annotations

import shutil
from pathlib import Path


def remove_legacy_gold_under_root(gold_root: Path) -> list[str]:
    """Delete legacy Gold paths under *gold_root* that are not part of the active contract.

    Removes:
    - ``latest_run_metadata/`` directly under *gold_root* (superseded by ``source=<slug>/latest_run_metadata/``).
    - Any directory whose name starts with ``ingest_month=`` anywhere under *gold_root*
      (active facts use ``posted_month=`` under ``<table>/source=<slug>/``).

    Returns a list of removed paths (as strings), newest/deepest first for nested dirs.
    """
    removed: list[str] = []
    if not gold_root.is_dir():
        return removed

    legacy_top = gold_root / "latest_run_metadata"
    if legacy_top.is_dir():
        shutil.rmtree(legacy_top)
        removed.append(str(legacy_top))

    legacy_ingest_month_dirs: list[Path] = []
    for p in gold_root.rglob("*"):
        if p.is_dir() and p.name.startswith("ingest_month="):
            legacy_ingest_month_dirs.append(p)
    for p in sorted(legacy_ingest_month_dirs, key=lambda x: len(x.parts), reverse=True):
        if p.exists():
            shutil.rmtree(p)
            removed.append(str(p))
    return removed


def remove_legacy_silver_flat_batches_under_jobs(jobs_root: Path) -> list[str]:
    """Remove pre–source-prefix Silver batches: ``jobs/ingest_date=.../`` (direct children only).

    Active layout is ``jobs/source=<slug>/ingest_date=.../run_id=.../``.
    """
    removed: list[str] = []
    if not jobs_root.is_dir():
        return removed
    for child in list(jobs_root.iterdir()):
        if child.is_dir() and child.name.startswith("ingest_date="):
            shutil.rmtree(child)
            removed.append(str(child))
    return removed
