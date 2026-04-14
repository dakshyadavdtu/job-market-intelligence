from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import Any

from src.jmi.config import AppConfig, DataPath


@dataclass
class ConnectorState:
    source_name: str
    last_successful_run_id: str | None
    last_successful_run_at: str | None
    fetch_watermark_created_at: int | None
    fallback_lookback_hours: int
    last_status: str
    incremental_strategy: str

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def default(cls, source_name: str, fallback_lookback_hours: int, incremental_strategy: str) -> "ConnectorState":
        return cls(
            source_name=source_name,
            last_successful_run_id=None,
            last_successful_run_at=None,
            fetch_watermark_created_at=None,
            fallback_lookback_hours=fallback_lookback_hours,
            last_status="unknown",
            incremental_strategy=incremental_strategy,
        )

    @classmethod
    def from_json_dict(cls, data: dict[str, Any], source_name: str, fallback_lookback_hours: int) -> "ConnectorState":
        strategy = str(data.get("incremental_strategy") or "fallback_lookback")
        wm = data.get("fetch_watermark_created_at")
        wm_int: int | None
        if wm is None:
            wm_int = None
        else:
            wm_int = int(wm)
        return cls(
            source_name=str(data.get("source_name") or source_name),
            last_successful_run_id=data.get("last_successful_run_id"),
            last_successful_run_at=data.get("last_successful_run_at"),
            fetch_watermark_created_at=wm_int,
            fallback_lookback_hours=int(data.get("fallback_lookback_hours") or fallback_lookback_hours),
            last_status=str(data.get("last_status") or "unknown"),
            incremental_strategy=strategy,
        )


def connector_state_path(cfg: AppConfig) -> DataPath:
    slice_tag = os.getenv("JMI_ARBEITNOW_SLICE", "").strip()
    if cfg.source_name == "arbeitnow" and slice_tag:
        return cfg.state_root / f"source={cfg.source_name}" / f"slice={slice_tag}" / "connector_state.json"
    return cfg.state_root / f"source={cfg.source_name}" / "connector_state.json"


def load_connector_state(cfg: AppConfig) -> ConnectorState:
    path = connector_state_path(cfg)
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        return ConnectorState.from_json_dict(data, cfg.source_name, cfg.incremental_lookback_hours)
    except Exception:
        return ConnectorState.default(
            cfg.source_name,
            cfg.incremental_lookback_hours,
            cfg.incremental_strategy_default,
        )


def save_connector_state(cfg: AppConfig, state: ConnectorState) -> str:
    path = connector_state_path(cfg)
    text = json.dumps(state.to_json_dict(), indent=2)
    path.write_text(text, encoding="utf-8")
    return str(path)
