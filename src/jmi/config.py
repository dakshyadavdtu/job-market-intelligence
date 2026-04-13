from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def _is_s3_uri(value: str) -> bool:
    return value.strip().lower().startswith("s3://")


@dataclass(frozen=True)
class DataPath:
    value: str

    @property
    def is_s3(self) -> bool:
        return _is_s3_uri(self.value)

    def __truediv__(self, other: object) -> "DataPath":
        part = str(other).lstrip("/")
        if self.value.endswith("/"):
            return DataPath(self.value + part)
        return DataPath(self.value + "/" + part)

    @property
    def parent(self) -> "DataPath":
        v = self.value.rstrip("/")
        if self.is_s3:
            # Keep "s3://bucket" as the floor.
            prefix = "s3://"
            rest = v[len(prefix) :]
            if "/" not in rest:
                return DataPath(v)
            bucket, _, key = rest.partition("/")
            if "/" not in key:
                return DataPath(f"s3://{bucket}")
            return DataPath(f"s3://{bucket}/{key.rsplit('/', 1)[0]}")
        p = Path(v).parent
        return DataPath(str(p))

    def as_path(self) -> Path:
        if self.is_s3:
            raise ValueError(f"Not a local path: {self.value}")
        return Path(self.value)

    def write_text(self, data: str, encoding: str = "utf-8") -> None:
        if self.is_s3:
            # Lazy import for local runs without AWS deps.
            import boto3  # type: ignore

            bucket, key = split_s3_uri(self.value)
            boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data.encode(encoding))
            return

        path = self.as_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(data, encoding=encoding)

    def read_text(self, encoding: str = "utf-8") -> str:
        if self.is_s3:
            import boto3  # type: ignore

            bucket, key = split_s3_uri(self.value)
            body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
            return body.decode(encoding)
        return self.as_path().read_text(encoding=encoding)

    def __str__(self) -> str:
        return self.value


def split_s3_uri(uri: str) -> tuple[str, str]:
    u = uri.strip()
    if not _is_s3_uri(u):
        raise ValueError(f"Not an s3 uri: {uri}")
    rest = u[5:]
    bucket, _, key = rest.partition("/")
    return bucket, key


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_optional_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    return int(raw)


@dataclass(frozen=True)
class AppConfig:
    project_name: str = "jmi"
    schema_version: str = "v1"
    source_name: str = "arbeitnow"
    data_root: DataPath = DataPath(os.getenv("JMI_DATA_ROOT", "data"))
    incremental_lookback_hours: int = _env_int("JMI_INCREMENTAL_LOOKBACK_HOURS", 48)
    # Case A: Unix epoch seconds; sent as min_created_at only if JMI_ARBEITNOW_USE_MIN_CREATED_AT_PARAM is enabled.
    arbeitnow_min_created_at: int | None = _env_optional_int("JMI_ARBEITNOW_MIN_CREATED_AT")
    arbeitnow_use_min_created_at_param: bool = os.getenv("JMI_ARBEITNOW_USE_MIN_CREATED_AT_PARAM", "").lower() in (
        "1",
        "true",
        "yes",
    )

    @property
    def bronze_root(self) -> DataPath:
        return self.data_root / "bronze"

    @property
    def silver_root(self) -> DataPath:
        return self.data_root / "silver"

    @property
    def gold_root(self) -> DataPath:
        return self.data_root / "gold"

    @property
    def gold_v2_root(self) -> DataPath:
        """Normalized presentation layer (yearly/monthly rollups); derived only from source-truth gold/."""
        return self.data_root / "gold_v2"

    @property
    def quality_root(self) -> DataPath:
        return self.data_root / "quality"

    @property
    def health_root(self) -> DataPath:
        return self.data_root / "health"

    @property
    def state_root(self) -> DataPath:
        return self.data_root / "state"

    @property
    def incremental_strategy_default(self) -> str:
        if self.arbeitnow_use_min_created_at_param and self.arbeitnow_min_created_at is not None:
            return "true_api_filter"
        return "fallback_lookback"

    def incremental_strategy_effective(self) -> str:
        """Strategy for Bronze incremental filter. Non-Arbeitnow sources always use client lookback."""
        if self.source_name == "arbeitnow":
            return self.incremental_strategy_default
        return "fallback_lookback"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_run_id() -> str:
    ts = utc_now().strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}-{uuid4().hex[:8]}"
