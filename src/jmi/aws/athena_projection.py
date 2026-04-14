"""Keep jmi_gold_v2 partition projection in sync with S3 run_id prefixes (Lambda-safe boto3)."""

from __future__ import annotations

import os
import time


def athena_output_uri() -> str:
    bucket = os.environ.get("JMI_BUCKET", "jmi-dakshyadav-job-market-intelligence").strip()
    return f"s3://{bucket}/athena-results/"


def collect_run_ids_from_s3_gold(
    bucket: str,
    *,
    prefix: str = "gold/role_demand_monthly/",
    region: str | None = None,
) -> list[str]:
    """Scan S3 keys under prefix; return sorted unique run_id segment values."""
    import boto3  # type: ignore

    client = boto3.client("s3", region_name=region or os.environ.get("AWS_REGION") or "ap-south-1")
    run_ids: set[str] = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            k = obj["Key"]
            if "run_id=" in k:
                part = k.split("run_id=", 1)[1].split("/", 1)[0]
                run_ids.add(part)
    return sorted(run_ids)


def _wait_athena_query(client: object, qid: str, region: str) -> None:
    for _ in range(120):
        st = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            return
        if st in ("FAILED", "CANCELLED"):
            reason = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"].get("Status", {}).get(
                "StateChangeReason", ""
            )
            raise RuntimeError(f"Athena {qid} {st}: {reason}")
        time.sleep(1)
    raise TimeoutError(qid)


# Must match ddl_gold_*.sql + deploy_athena_v2.GOLD_V2_RUN_PROJECTION_TABLES
GOLD_V2_RUN_PROJECTION_TABLES = (
    "skill_demand_monthly",
    "role_demand_monthly",
    "location_demand_monthly",
    "company_hiring_monthly",
    "pipeline_run_summary",
)


def update_gold_v2_run_id_projection(
    run_id_csv: str,
    *,
    region: str,
    workgroup: str,
) -> None:
    """Set Glue projection.run_id.values on all jmi_gold_v2 fact tables (comma-separated run_ids)."""
    if "'" in run_id_csv:
        raise ValueError("run_id CSV must not contain single quotes")
    import boto3  # type: ignore

    client = boto3.client("athena", region_name=region)
    out = athena_output_uri()
    for name in GOLD_V2_RUN_PROJECTION_TABLES:
        sql = (
            f"ALTER TABLE jmi_gold_v2.{name} SET TBLPROPERTIES "
            f"('projection.run_id.values'='{run_id_csv}')"
        )
        r = client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": "jmi_gold_v2"},
            ResultConfiguration={"OutputLocation": out},
            WorkGroup=workgroup,
        )
        qid = r["QueryExecutionId"]
        _wait_athena_query(client, qid, region)


def sync_gold_run_id_projection_from_s3(
    *,
    bucket: str | None = None,
    region: str | None = None,
    workgroup: str = "primary",
) -> str:
    """List all run_ids under gold/role_demand_monthly/, update Glue projection. Returns CSV used."""
    b = (bucket or os.environ.get("JMI_BUCKET", "").strip()) or "jmi-dakshyadav-job-market-intelligence"
    reg = region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
    ids = collect_run_ids_from_s3_gold(b, region=reg)
    if not ids:
        raise RuntimeError(f"No run_id= segments found under s3://{b}/gold/role_demand_monthly/")
    csv = ",".join(ids)
    update_gold_v2_run_id_projection(csv, region=reg, workgroup=workgroup)
    return csv
