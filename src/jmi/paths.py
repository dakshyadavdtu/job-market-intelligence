"""
Canonical data lake path layout (local + S3).

Medallion layers are source-partitioned under Bronze/Silver/Gold so Arbeitnow, Adzuna,
and future sources do not share ambiguous prefixes.

Layout (examples):
  bronze/source=<slug>/ingest_date=YYYY-MM-DD/run_id=<id>/raw.jsonl.gz
  silver/jobs/source=<slug>/ingest_date=.../run_id=.../part-00001.parquet
  silver/jobs/source=<slug>/merged/latest.parquet
  silver_legacy/jobs/ingest_date=.../run_id=.../part-00001.parquet  (Arbeitnow flat batches archived here)
  gold/<table>/source=<slug>/posted_month=YYYY-MM/run_id=<id>/part-00001.parquet
  gold/source=<slug>/latest_run_metadata/part-00001.parquet
  gold_legacy/<table>/...  (archived ingest_month= partitions only; not written by current pipeline)

Gold v2 presentation (teacher-friendly; same tree for both sources; truthful scope only):
  gold_v2/presentation/<v2_fact>/monthly/source=<slug>/posted_month=YYYY-MM/part-00001.parquet
  gold_v2/presentation/<v2_fact>/yearly/source=<slug>/year=YYYY/part-00001.parquet

Legacy comparison parquet prefixes (orphaned if present; not written by the pipeline — use Athena
``jmi_analytics_v2.comparison_*`` and ``v2_*`` views; see ``infra/aws/athena/legacy_comparison_gold_parquet_paths.txt``):
  gold/comparison_strict_common_month/…
  gold/comparison_yearly/…
  gold/comparison_march_only/…
"""

from __future__ import annotations

from src.jmi.config import AppConfig, DataPath


def bronze_raw_gz(cfg: AppConfig, ingest_date: str, run_id: str) -> DataPath:
    return cfg.bronze_root / f"source={cfg.source_name}" / f"ingest_date={ingest_date}" / f"run_id={run_id}" / "raw.jsonl.gz"


def silver_jobs_merged_latest(cfg: AppConfig) -> DataPath:
    return cfg.silver_root / "jobs" / f"source={cfg.source_name}" / "merged" / "latest.parquet"


def silver_legacy_flat_jobs_root(cfg: AppConfig) -> DataPath:
    """Pre–source-prefix Arbeitnow batches (flat ingest_date/run_id), kept out of silver/jobs/."""
    return cfg.data_root / "silver_legacy" / "jobs"


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
    posted_month: str,
    pipeline_run_id: str,
) -> DataPath:
    """Gold fact path: analysis grain is **posted_month** (job posting month from Silver `posted_at`)."""
    return (
        cfg.gold_root
        / table_name
        / f"source={cfg.source_name}"
        / f"posted_month={posted_month}"
        / f"run_id={pipeline_run_id}"
        / "part-00001.parquet"
    )


def gold_latest_run_metadata_file(cfg: AppConfig) -> DataPath:
    """Single-row pointer Parquet for one pipeline source (Athena ``latest_run_metadata`` helpers).

    Always ``gold/source=<slug>/latest_run_metadata/part-00001.parquet``. Never a legacy top-level
    ``gold/latest_run_metadata/`` (no ``source=`` segment).
    """
    slug = str(cfg.source_name).strip()
    if not slug:
        raise ValueError("AppConfig.source_name must be non-empty to write latest_run_metadata")
    source_seg = f"source={slug}"
    out = cfg.gold_root / source_seg / "latest_run_metadata" / "part-00001.parquet"
    normalized = str(out).replace("\\", "/")
    if f"/{source_seg}/latest_run_metadata/" not in normalized:
        raise RuntimeError(f"refusing non-source-scoped latest_run_metadata path: {out}")
    return out


def gold_v2_presentation_monthly_parquet(
    cfg: AppConfig,
    presentation_fact: str,
    *,
    source_slug: str,
    posted_month: str,
) -> DataPath:
    """e.g. presentation_fact=v2_skill_demand → gold_v2/presentation/v2_skill_demand/monthly/source=.../posted_month=..."""
    return (
        cfg.gold_v2_root
        / "presentation"
        / presentation_fact
        / "monthly"
        / f"source={source_slug}"
        / f"posted_month={posted_month}"
        / "part-00001.parquet"
    )


def gold_v2_presentation_yearly_parquet(
    cfg: AppConfig,
    presentation_fact: str,
    *,
    source_slug: str,
    calendar_year: str,
) -> DataPath:
    return (
        cfg.gold_v2_root
        / "presentation"
        / presentation_fact
        / "yearly"
        / f"source={source_slug}"
        / f"year={calendar_year}"
        / "part-00001.parquet"
    )
