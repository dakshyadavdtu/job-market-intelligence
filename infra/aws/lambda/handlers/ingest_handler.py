from __future__ import annotations

import json
import os

import boto3

from src.jmi.pipelines.ingest_live import run as ingest_run

lambda_client = boto3.client("lambda")


def handler(event, context):
    result = ingest_run()

    silver_fn = os.environ["JMI_SILVER_FUNCTION_NAME"]
    payload = {"bronze_file": result["bronze_data_file"], "run_id": result["run_id"]}

    lambda_client.invoke(
        FunctionName=silver_fn,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )

    return {"statusCode": 200, "body": json.dumps(result)}

