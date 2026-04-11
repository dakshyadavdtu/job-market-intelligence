from __future__ import annotations

import json

from src.jmi.pipelines.transform_gold import run as gold_run


def handler(event, context):
    result = gold_run(
        silver_file=event.get("silver_file"),
        merged_silver_file=event.get("merged_silver_file"),
        pipeline_run_id=event.get("run_id"),
    )
    return {"statusCode": 200, "body": json.dumps(result)}

