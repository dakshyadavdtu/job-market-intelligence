#!/usr/bin/env python3
"""
One-command local pipeline + S3 + Athena v2 projection + verification.

jmi_gold_v2 fact tables use Glue partition projection: new S3 run_id prefixes are invisible until
`projection.run_id.values` is updated (done here after `aws s3 sync`). latest_run_metadata reads the
synced Parquet path; verification retries briefly for propagation.

Usage (from repo root, venv active):
  python scripts/pipeline_live_sync.py arbeitnow
  python scripts/pipeline_live_sync.py adzuna_in
  ./scripts/run_pipeline_live_sync.sh arbeitnow

Environment:
  JMI_BUCKET   — S3 bucket (default: same as deploy_athena_v2.BUCKET)
  AWS_DEFAULT_REGION or AWS_REGION — default ap-south-1

Opt-out flags (default: do everything):
  --skip-sync-s3
  --skip-athena
  --skip-verify
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from src.jmi.storage_layout import (
    remove_legacy_gold_under_root,
    remove_legacy_silver_flat_batches_under_jobs,
)


REPO_ROOT = Path(__file__).resolve().parents[1]

# Exclude legacy keys from aws s3 sync (belt-and-suspenders after local purge).
_SILVER_SYNC_EXCLUDES = ("--exclude", "jobs/ingest_date=*")
# Do not add "latest_run_metadata/*" — that would also exclude gold/source=<slug>/latest_run_metadata/.
_GOLD_SYNC_EXCLUDES = ("--exclude", "*ingest_month*",)


def _load_deploy_athena_v2():
    path = REPO_ROOT / "scripts" / "deploy_athena_v2.py"
    spec = importlib.util.spec_from_file_location("deploy_athena_v2", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_s3_purge_legacy_gold():
    path = REPO_ROOT / "scripts" / "s3_purge_legacy_gold_active.py"
    spec = importlib.util.spec_from_file_location("s3_purge_legacy_gold_active", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _pipeline_subprocess_env(data_root: Path) -> dict:
    """Force local repo `data/` for ingest/silver/gold so outputs match `aws s3 sync` sources.

    If the shell has `JMI_DATA_ROOT=s3://...`, subprocesses would write directly to S3 while this
    script syncs `data/bronze|silver|gold` — new run_ids would never appear under the synced prefixes.

    Live sync defaults Gold to incremental **one posted month** (``JMI_GOLD_INCREMENTAL_POSTED_MONTHS``,
    default ``2026-04``) so each run does not rewrite every historical ``posted_month=`` folder.
    Set ``JMI_GOLD_FULL_MONTHS=1`` or export a different ``JMI_GOLD_INCREMENTAL_POSTED_MONTHS`` to override.
    """
    env = os.environ.copy()
    env["JMI_DATA_ROOT"] = str(data_root.resolve())
    if "JMI_GOLD_INCREMENTAL_POSTED_MONTHS" not in env:
        if os.environ.get("JMI_GOLD_FULL_MONTHS", "").strip().lower() not in ("1", "true", "yes"):
            env["JMI_GOLD_INCREMENTAL_POSTED_MONTHS"] = "2026-04"
    return env


def _run_module_json(mod: str, extra: list[str], *, env: dict | None = None) -> dict:
    cmd = [sys.executable, "-m", mod, *extra]
    print(f"→ {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, env=env)
    if r.returncode != 0:
        sys.stderr.write(r.stderr)
        sys.stdout.write(r.stdout)
        raise RuntimeError(f"Command failed (exit {r.returncode}): {' '.join(cmd)}")
    out = r.stdout.strip()
    if not out:
        raise RuntimeError(f"No stdout from {mod}")
    return json.loads(out)


def _scan_gold_run_ids(gold_root: Path) -> set[str]:
    if not gold_root.is_dir():
        return set()
    found: set[str] = set()
    for p in gold_root.rglob("run_id=*"):
        if p.is_dir() and p.name.startswith("run_id="):
            found.add(p.name.split("=", 1)[1])
    return found


def _merge_run_id_csv(existing_csv: str, pipeline_run_id: str, gold_root: Path) -> str:
    ids = {x.strip() for x in existing_csv.split(",") if x.strip()}
    ids.update(_scan_gold_run_ids(gold_root))
    if pipeline_run_id:
        ids.add(pipeline_run_id.strip())
    return ",".join(sorted(ids))


def _aws_s3_sync(
    deploy,
    local_dir: Path,
    s3_prefix: str,
    region: str,
    *,
    extra_sync_args: tuple[str, ...] = (),
) -> None:
    if not local_dir.is_dir():
        print(f"  (skip sync: not a directory: {local_dir})", flush=True)
        return
    bucket = deploy.athena_bucket()
    dest = f"s3://{bucket}/{s3_prefix.rstrip('/')}/"
    cmd = ["aws", "s3", "sync", str(local_dir), dest, "--region", region, *extra_sync_args]
    print(f"→ {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(r.stderr)
        sys.stdout.write(r.stdout)
        raise RuntimeError(f"aws s3 sync failed ({r.returncode})")
    if r.stdout.strip():
        print(r.stdout.rstrip(), flush=True)


def _athena_scalar(
    deploy,
    sql: str,
    *,
    region: str,
    workgroup: str,
    database: str = "jmi_gold_v2",
) -> str:
    qid = deploy.run_athena_sql(
        sql,
        region=region,
        workgroup=workgroup,
        database=database,
    )
    deploy.wait_done(qid, region)
    out = subprocess.check_output(
        [
            "aws",
            "athena",
            "get-query-results",
            "--region",
            region,
            "--query-execution-id",
            qid,
        ],
        text=True,
    )
    data = json.loads(out)
    rows = data.get("ResultSet", {}).get("Rows") or []
    if len(rows) < 2:
        raise RuntimeError(f"Unexpected Athena result rows for {qid}: {rows!r}")
    cell = rows[1]["Data"][0].get("VarCharValue", "")
    return str(cell)


def _verify_athena_post_sync(
    deploy,
    *,
    src: str,
    prid: str,
    region: str,
    workgroup: str,
) -> None:
    """One SQL statement per round trip; retry briefly for S3/Glue propagation."""
    esc = prid.replace("'", "''")
    esc_src = src.replace("'", "''")
    meta_table = "latest_run_metadata" if src == "arbeitnow" else "latest_run_metadata_adzuna"
    steps: list[tuple[str, str]] = [
        (
            "skill_demand_monthly",
            f"SELECT CAST(COUNT(*) AS VARCHAR) AS n FROM jmi_gold_v2.skill_demand_monthly "
            f"WHERE source = '{esc_src}' AND run_id = '{esc}'",
        ),
        (
            "pipeline_run_summary",
            f"SELECT CAST(COUNT(*) AS VARCHAR) AS n FROM jmi_gold_v2.pipeline_run_summary "
            f"WHERE source = '{esc_src}' AND run_id = '{esc}'",
        ),
        (
            "role_demand_monthly",
            f"SELECT CAST(COUNT(*) AS VARCHAR) AS n FROM jmi_gold_v2.role_demand_monthly "
            f"WHERE source = '{esc_src}' AND run_id = '{esc}'",
        ),
        (
            f"latest_run_metadata ({meta_table})",
            f"SELECT run_id FROM jmi_gold_v2.{meta_table} LIMIT 1",
        ),
    ]

    for attempt in range(1, 6):
        print(f"\n--- Athena verify (attempt {attempt}/5) ---", flush=True)
        out: dict[str, str] = {}
        for label, sql in steps:
            print(f"  → {label}", flush=True)
            val = _athena_scalar(deploy, sql, region=region, workgroup=workgroup, database="jmi_gold_v2")
            print(f"     {val}", flush=True)
            key = label.split()[0]
            if key in ("skill_demand_monthly", "pipeline_run_summary", "role_demand_monthly"):
                out[key] = val
            elif "latest_run_metadata" in label:
                out["metadata_run_id"] = val

        meta_ok = (out.get("metadata_run_id") or "").strip() == prid
        try:
            n_skill = int(out["skill_demand_monthly"])
            n_pipe = int(out["pipeline_run_summary"])
            n_role = int(out["role_demand_monthly"])
        except (KeyError, ValueError):
            n_skill = n_pipe = n_role = -1

        # Summary must exist; at least one dimension table should have rows for this run.
        facts_ok = n_pipe >= 1 and (n_skill >= 1 or n_role >= 1)

        if meta_ok and facts_ok:
            print("Athena verify: OK", flush=True)
            return
        if attempt < 5:
            print("  (retrying after S3/projection propagation…)", flush=True)
            time.sleep(3)

    raise RuntimeError(
        "Athena verification failed after retries: expected projection-visible rows for this run_id and "
        f"latest_run_metadata.run_id={prid!r} (source={src!r})."
    )


def main() -> int:
    deploy = _load_deploy_athena_v2()

    p = argparse.ArgumentParser(description="Run pipeline locally, sync to S3, update Athena v2 run_id projection.")
    p.add_argument(
        "source",
        choices=("arbeitnow", "adzuna_in", "adzuna"),
        help="Data source (adzuna is an alias for adzuna_in).",
    )
    p.add_argument("--skip-sync-s3", action="store_true", help="Do not aws s3 sync data/* to the bucket.")
    p.add_argument("--skip-athena", action="store_true", help="Do not ALTER TABLE projection.run_id.values.")
    p.add_argument("--skip-verify", action="store_true", help="Do not run Athena COUNT(*) checks.")
    p.add_argument(
        "--skip-legacy-purge",
        action="store_true",
        help="Do not delete legacy gold/silver paths on disk before sync (sync excludes still apply).",
    )
    p.add_argument(
        "--skip-purge-s3-legacy",
        action="store_true",
        help="After sync, do not delete remote gold/ orphans (ingest_month=, gold/latest_run_metadata/).",
    )
    p.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1")
    p.add_argument("--workgroup", default="primary")
    args = p.parse_args()

    src = "adzuna_in" if args.source == "adzuna" else args.source
    data_root = REPO_ROOT / "data"
    penv = _pipeline_subprocess_env(data_root)

    print("=== JMI pipeline live sync ===", flush=True)
    print(f"source={src}  region={args.region}  bucket={deploy.athena_bucket()}", flush=True)
    print(f"JMI_DATA_ROOT for pipeline steps (forced): {data_root.resolve()}", flush=True)

    # 1–3 Pipeline
    if src == "arbeitnow":
        ingest = _run_module_json("src.jmi.pipelines.ingest_live", [], env=penv)
        silver = _run_module_json("src.jmi.pipelines.transform_silver", [], env=penv)
        gold = _run_module_json("src.jmi.pipelines.transform_gold", [], env=penv)
    else:
        ingest = _run_module_json("src.jmi.pipelines.ingest_adzuna", [], env=penv)
        silver = _run_module_json("src.jmi.pipelines.transform_silver", ["--source", "adzuna_in"], env=penv)
        gold = _run_module_json("src.jmi.pipelines.transform_gold", ["--source", "adzuna_in"], env=penv)

    prid = gold.get("pipeline_run_id")
    if not prid:
        raise RuntimeError(f"Gold stage did not return pipeline_run_id: {gold!r}")

    print("\n--- Lineage ---", flush=True)
    print(f"bronze_run_id (ingest): {ingest.get('run_id')}", flush=True)
    print(f"pipeline_run_id (gold): {prid}", flush=True)
    print(f"silver merged file:     {silver.get('merged_silver_file')}", flush=True)
    print(f"posted_months_rebuilt:  {gold.get('posted_months_rebuilt')}", flush=True)

    run_id_csv = _merge_run_id_csv(deploy.RUN_ID_ENUM, prid, data_root / "gold")

    # 4–5 Purge legacy local paths (before S3 sync when enabled; ingest_month never valid in active gold)
    if not args.skip_legacy_purge:
        print("\n--- Legacy layout purge (local, active roots only) ---", flush=True)
        g_removed = remove_legacy_gold_under_root(data_root / "gold")
        s_removed = remove_legacy_silver_flat_batches_under_jobs(data_root / "silver" / "jobs")
        if g_removed:
            print(f"  removed {len(g_removed)} gold path(s)", flush=True)
            for p in g_removed[:20]:
                print(f"    {p}", flush=True)
            if len(g_removed) > 20:
                print(f"    ... and {len(g_removed) - 20} more", flush=True)
        else:
            print("  (no legacy gold paths removed)", flush=True)
        if s_removed:
            print(f"  removed {len(s_removed)} silver flat batch path(s)", flush=True)
            for p in s_removed[:20]:
                print(f"    {p}", flush=True)
        else:
            print("  (no legacy silver flat paths removed)", flush=True)

    if not args.skip_sync_s3:
        print("\n--- S3 sync ---", flush=True)
        _aws_s3_sync(deploy, data_root / "bronze", "bronze", args.region)
        _aws_s3_sync(deploy, data_root / "silver", "silver", args.region, extra_sync_args=_SILVER_SYNC_EXCLUDES)
        _aws_s3_sync(deploy, data_root / "gold", "gold", args.region, extra_sync_args=_GOLD_SYNC_EXCLUDES)
        derived = data_root / "derived"
        if derived.is_dir() and any(derived.iterdir()):
            _aws_s3_sync(deploy, derived, "derived", args.region)
        state_dir = data_root / "state"
        if state_dir.is_dir():
            _aws_s3_sync(deploy, state_dir, "state", args.region)
        print("S3 sync: OK", flush=True)

        if not args.skip_purge_s3_legacy:
            print("\n--- S3 legacy gold purge (remote orphans) ---", flush=True)
            purge_mod = _load_s3_purge_legacy_gold()
            pr = purge_mod.purge_legacy_gold_active(deploy.athena_bucket(), dry_run=False)
            print(json.dumps(pr, indent=2), flush=True)
        else:
            print("\n--- S3 legacy gold purge: skipped ---", flush=True)
    else:
        print("\n--- S3 sync: skipped ---", flush=True)

    # 6 Athena projection
    if not args.skip_athena:
        print("\n--- Athena jmi_gold_v2 projection (run_id) ---", flush=True)
        deploy.update_gold_v2_run_id_projection(
            run_id_csv,
            region=args.region,
            workgroup=args.workgroup,
        )
        print("Athena projection update: OK", flush=True)
    else:
        print("\n--- Athena projection: skipped ---", flush=True)

    # 7 Verify (requires S3 sync + projection update so Athena can see new partitions)
    if not args.skip_verify:
        if args.skip_sync_s3:
            print("\n--- Athena verify: skipped (no S3 sync this run) ---", flush=True)
        elif args.skip_athena:
            print("\n--- Athena verify: skipped (--skip-athena; projection not updated) ---", flush=True)
        else:
            _verify_athena_post_sync(
                deploy,
                src=src,
                prid=prid,
                region=args.region,
                workgroup=args.workgroup,
            )
    else:
        print("\n--- Athena verify: skipped ---", flush=True)

    print("\n=== Summary ===", flush=True)
    print(f"run_id (live partition key): {prid}", flush=True)
    print(f"local data root:               {data_root}", flush=True)
    print(f"projection run_id count:       {len(run_id_csv.split(','))}", flush=True)
    print("DONE", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        raise SystemExit(130)
