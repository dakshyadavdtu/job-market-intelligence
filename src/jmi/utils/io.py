from __future__ import annotations

import io
import gzip
import json
from pathlib import Path
from typing import Iterable, Mapping, Union

import pandas as pd

from src.jmi.config import DataPath, split_s3_uri


PathLike = Union[Path, DataPath, str]


def _to_datapath(path: PathLike) -> DataPath:
    if isinstance(path, DataPath):
        return path
    if isinstance(path, Path):
        return DataPath(str(path))
    return DataPath(str(path))


def ensure_dir(path: PathLike) -> None:
    p = _to_datapath(path)
    if p.is_s3:
        return
    p.as_path().mkdir(parents=True, exist_ok=True)


def write_jsonl_gz(path: PathLike, records: Iterable[Mapping]) -> None:
    p = _to_datapath(path)
    if p.is_s3:
        import boto3  # type: ignore

        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            for row in records:
                line = (json.dumps(row, ensure_ascii=True) + "\n").encode("utf-8")
                gz.write(line)
        bucket, key = split_s3_uri(str(p))
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
        return

    ensure_dir(p.parent)
    with gzip.open(p.as_path(), "wt", encoding="utf-8") as f:
        for row in records:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")


def read_jsonl_gz(path: PathLike) -> list[dict]:
    p = _to_datapath(path)
    rows: list[dict] = []
    if p.is_s3:
        import boto3  # type: ignore

        bucket, key = split_s3_uri(str(p))
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        with gzip.GzipFile(fileobj=io.BytesIO(body), mode="rb") as gz:
            for line in gz.read().decode("utf-8").splitlines():
                if line:
                    rows.append(json.loads(line))
        return rows

    with gzip.open(p.as_path(), "rt", encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    return rows


def write_parquet(path: PathLike, df: pd.DataFrame) -> None:
    p = _to_datapath(path)
    if p.is_s3:
        df.to_parquet(str(p), index=False)
        return
    ensure_dir(p.parent)
    df.to_parquet(p.as_path(), index=False)
