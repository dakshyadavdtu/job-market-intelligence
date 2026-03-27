from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


@dataclass(frozen=True)
class AppConfig:
    project_name: str = "jmi"
    schema_version: str = "v1"
    source_name: str = "arbeitnow"
    data_root: Path = Path(os.getenv("JMI_DATA_ROOT", "data"))

    @property
    def bronze_root(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_root(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_root(self) -> Path:
        return self.data_root / "gold"

    @property
    def quality_root(self) -> Path:
        return self.data_root / "quality"

    @property
    def health_root(self) -> Path:
        return self.data_root / "health"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_run_id() -> str:
    ts = utc_now().strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}-{uuid4().hex[:8]}"
