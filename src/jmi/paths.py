"""
Canonical data lake path layout (local + S3).

Medallion layers are source-partitioned under Bronze/Silver/Gold so Arbeitnow, Adzuna,
and future sources do not share ambiguous prefixes.

Layout (examples):
  bronze/source=<slug>/ingest_date=YYYY-MM-DD/run_id=<id>/raw.jsonl.gz
  silver/jobs/source=<slug>/ingest_date=.../run_id=.../part-00001.parquet
  silver/jobs/source=<slug>/merged/latest.parquet
  gold/<table>/source=<slug>/ingest_month=YYYY-MM/run_id=<id>/part-00001.parquet
  gold/source=<slug>/latest_run_metadata/part-00001.parquet

Derived / comparison outputs (not source-native facts):
  derived/comparison/...
"""

from __future__ import annotations

from src.jmi.config import AppConfig, DataPath


def bronze_raw_gz(cfg: AppConfig, ingest_date: str, run_id: str) -> DataPath:
    return cfg.bronze_root / f"source={cfg.source_name}" / f"ingest_date={ingest_date}" / f"run_id={run_id}" / "raw.jsonl.gz"


def silver_jobs_merged_latest(cfg: AppConfig) -> DataPath:
    return cfg.silver_root / "jobs" / f"source={cfg.source_name}" / "merged" / "latest.parquet"


def silver_jobs_batch_part(cfg: AppConfig, ingest_date: str, bronze_run_id: str) -> DataPath:
    """Per-batch Silver Parquet (canonical modular layout for all sources)."""
    return (
        cfg.silver_root
        / "jobs"
        / f"source={cfg.source_name}"
        / f"ingest_date={ingest_date}"
        / f"run_id={bronze_run_id}"
        / "part-00001.parquet"
    )


def gold_fact_partition(
    cfg: AppConfig,
    table_name: str,
    *,
    ingest_month: str,
    pipeline_run_id: str,
) -> DataPath:
    """Gold fact path: table-first, then source partition (matches Glue LOCATION .../gold/<table>/)."""
    return (
        cfg.gold_root
        / table_name
        / f"source={cfg.source_name}"
        / f"ingest_month={ingest_month}"
        / f"run_id={pipeline_run_id}"
        / "part-00001.parquet"
    )


def gold_latest_run_metadata_file(cfg: AppConfig) -> DataPath:
    """Single-row pointer Parquet for this source (Athena latest-run helpers)."""
    return cfg.gold_root / f"source={cfg.source_name}" / "latest_run_metadata" / "part-00001.parquet"


def derived_comparison_root(cfg: AppConfig) -> DataPath:
    """Benchmark / combined outputs — not mixed into source-native Gold facts."""
    return cfg.data_root / "derived" / "comparison"
