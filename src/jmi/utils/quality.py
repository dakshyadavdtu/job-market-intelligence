from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class QualityReport:
    status: str
    checks_passed: int
    checks_failed: int
    row_count: int
    missing_title: int
    missing_company: int
    duplicate_job_id: int
    duplicate_source_key: int


def _missing_text_count(df: pd.DataFrame, column: str) -> int:
    if column not in df:
        return 0
    as_text = df[column].fillna("").astype(str)
    return int((as_text.str.strip() == "").sum())


def run_silver_checks(df: pd.DataFrame, bronze_row_count: int) -> QualityReport:
    row_count = int(len(df))
    missing_title = _missing_text_count(df, "title_norm")
    if missing_title == 0 and "title_raw" in df.columns:
        missing_title = _missing_text_count(df, "title_raw")
    if missing_title == 0 and "title_clean" in df.columns:
        missing_title = _missing_text_count(df, "title_clean")
    missing_company = _missing_text_count(df, "company_norm")
    if missing_company == 0 and "company_raw" in df.columns:
        missing_company = _missing_text_count(df, "company_raw")
    if missing_company == 0 and "company_name" in df.columns:
        missing_company = _missing_text_count(df, "company_name")
    duplicate_job_id = int(df["job_id"].duplicated().sum()) if "job_id" in df else 0
    duplicate_source_key = 0
    if "source" in df.columns and "job_id" in df.columns:
        n = len(df)
        if "source_job_id" in df.columns:
            sj = df["source_job_id"].fillna("").astype(str).str.strip()
        else:
            sj = pd.Series([""] * n, index=df.index)
        jid = df["job_id"].fillna("").astype(str)
        key = np.where(sj.to_numpy() != "", sj.to_numpy(), jid.to_numpy())
        duplicate_source_key = int(pd.DataFrame({"source": df["source"].astype(str), "_k": key}).duplicated().sum())
    elif {"source", "source_record_key"}.issubset(df.columns):
        duplicate_source_key = int(df[["source", "source_record_key"]].duplicated().sum())

    failed = 0
    failed += int(row_count == 0)
    failed += int(bronze_row_count > 0 and row_count == 0)
    failed += int(missing_title > 0)
    failed += int(missing_company > 0)
    failed += int(duplicate_job_id > 0)
    failed += int(duplicate_source_key > 0)
    total_checks = 6

    return QualityReport(
        status="PASS" if failed == 0 else "FAIL",
        checks_passed=total_checks - failed,
        checks_failed=failed,
        row_count=row_count,
        missing_title=missing_title,
        missing_company=missing_company,
        duplicate_job_id=duplicate_job_id,
        duplicate_source_key=duplicate_source_key,
    )
