from __future__ import annotations

import json

from src.jmi.pipelines.transform_gold import run as gold_run


def handler(event, context):
    silver_file = event.get("silver_file")
    result = gold_run(silver_file=silver_file)
    return {"statusCode": 200, "body": json.dumps(result)}

