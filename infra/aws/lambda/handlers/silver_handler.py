from __future__ import annotations

import json
import os

import boto3

from src.jmi.pipelines.transform_silver import run as silver_run

lambda_client = boto3.client("lambda")


def handler(event, context):
    bronze_file = event.get("bronze_file")
    result = silver_run(bronze_file=bronze_file)

    gold_fn = os.environ["JMI_GOLD_FUNCTION_NAME"]
    run_id = event.get("run_id") or result["bronze_run_id"]
    payload = {
        "silver_file": result["output_file"],
        "merged_silver_file": result.get("merged_silver_file"),
        "run_id": run_id,
    }

    lambda_client.invoke(
        FunctionName=gold_fn,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )

    return {"statusCode": 200, "body": json.dumps(result)}

