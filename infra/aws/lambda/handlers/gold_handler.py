from __future__ import annotations

import json
import os
from dataclasses import replace

from src.jmi.config import AppConfig
from src.jmi.pipelines.transform_gold import run as gold_run


def handler(event, context):
    ev = event or {}
    if ev.get("full_gold_months") or ev.get("full_months"):
        os.environ["JMI_GOLD_FULL_MONTHS"] = "1"
    elif "incremental_posted_months" in ev and ev.get("incremental_posted_months") is not None:
        os.environ["JMI_GOLD_INCREMENTAL_POSTED_MONTHS"] = str(ev["incremental_posted_months"]).strip()
    else:
        os.environ.setdefault("JMI_GOLD_INCREMENTAL_POSTED_MONTHS", "2026-04")
        os.environ.pop("JMI_GOLD_FULL_MONTHS", None)

    source = str(ev.get("source_name") or os.environ.get("JMI_SOURCE_NAME") or "arbeitnow").strip()
    cfg = replace(AppConfig(), source_name=source)
    result = gold_run(
        silver_file=ev.get("silver_file"),
        merged_silver_file=ev.get("merged_silver_file"),
        pipeline_run_id=ev.get("run_id"),
        cfg=cfg,
    )
    return {"statusCode": 200, "body": json.dumps(result)}

