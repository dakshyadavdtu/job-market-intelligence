from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_jsonl_gz(path: Path, records: Iterable[Mapping]) -> None:
    ensure_dir(path.parent)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in records:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")


def read_jsonl_gz(path: Path) -> list[dict]:
    rows: list[dict] = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    return rows


def write_parquet(path: Path, df: pd.DataFrame) -> None:
    ensure_dir(path.parent)
    df.to_parquet(path, index=False)
