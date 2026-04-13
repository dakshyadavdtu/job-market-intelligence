from __future__ import annotations

import json
import os
from dataclasses import replace

from src.jmi.config import AppConfig
from src.jmi.pipelines.transform_gold import run as gold_run


def handler(event, context):
    ev = event or {}
    source = str(ev.get("source_name") or os.environ.get("JMI_SOURCE_NAME") or "arbeitnow").strip()
    cfg = replace(AppConfig(), source_name=source)
    result = gold_run(
        silver_file=ev.get("silver_file"),
        merged_silver_file=ev.get("merged_silver_file"),
        pipeline_run_id=ev.get("run_id"),
        cfg=cfg,
    )
    return {"statusCode": 200, "body": json.dumps(result)}

