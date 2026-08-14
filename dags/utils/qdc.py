"""
Quality Data Control (QDC) module.

Implements four validation dimensions per skripsi Bab 2.6:
- completeness : required columns must not be null
- uniqueness   : no unintended duplicate keys
- consistency  : column dtypes match expected schema
- validity     : values respect business logic (e.g. price >= 0)

Each check returns a boolean mask (True = passes) so failing rows can be
routed to error_log instead of silently dropped.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger(__name__)

FAIL_FAST_THRESHOLD = 0.05  # 5%, per Bab 3.5.1
QDC_SUCCESS_TARGET = 0.95   # 95%, per Bab 1.3 / Bab 3.6.2


@dataclass
class QDCResult:
    table_name: str
    total_records: int
    passed_records: int
    failed_records: int
    failure_reasons: dict = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        if self.total_records == 0:
            return 1.0
        return self.passed_records / self.total_records

    @property
    def meets_target(self) -> bool:
        return self.success_rate >= QDC_SUCCESS_TARGET

    @property
    def triggers_fail_fast(self) -> bool:
        failure_rate = 1 - self.success_rate
        return failure_rate > FAIL_FAST_THRESHOLD

    def summary(self) -> str:
        return (
            f"[QDC:{self.table_name}] "
            f"total={self.total_records} passed={self.passed_records} "
            f"failed={self.failed_records} "
            f"success_rate={self.success_rate:.2%} "
            f"meets_target={self.meets_target}"
        )


def check_completeness(df: pd.DataFrame, required_cols: list[str]) -> pd.Series:
    """True where ALL required columns are non-null for that row."""
    return df[required_cols].notna().all(axis=1)


def check_uniqueness(df: pd.DataFrame, key_cols: list[str]) -> pd.Series:
    """True where the key combination is not a duplicate (keeps first occurrence)."""
    return ~df.duplicated(subset=key_cols, keep="first")


def check_consistency(df: pd.DataFrame, expected_dtypes: dict[str, str]) -> pd.Series:
    """
    True where value can be safely coerced to the expected dtype.
    Uses per-column coercion checks rather than a single dtype comparison,
    since raw CSV columns commonly load as 'object'.
    """
    mask = pd.Series(True, index=df.index)
    for col, dtype in expected_dtypes.items():
        if col not in df.columns:
            continue
        if dtype in ("float64", "int64"):
            coerced = pd.to_numeric(df[col], errors="coerce")
            mask &= coerced.notna() | df[col].isna()
        elif dtype == "datetime64[ns]":
            coerced = pd.to_datetime(df[col], errors="coerce")
            mask &= coerced.notna() | df[col].isna()
    return mask


def check_validity(df: pd.DataFrame, rules: dict[str, callable]) -> pd.Series:
    """
    True where each business-rule callable returns True for that row's value.
    rules: {column_name: lambda series -> boolean_series}
    Example: {"price": lambda s: s >= 0}
    """
    mask = pd.Series(True, index=df.index)
    for col, rule_fn in rules.items():
        if col not in df.columns:
            continue
        mask &= rule_fn(df[col]).fillna(False)
    return mask


def run_qdc(
    df: pd.DataFrame,
    table_name: str,
    required_cols: list[str],
    key_cols: list[str],
    expected_dtypes: dict[str, str] | None = None,
    validity_rules: dict[str, callable] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, QDCResult]:
    """
    Runs all four QDC dimensions and splits the dataframe into (passed, failed).

    Returns:
        passed_df: rows that satisfy every dimension
        failed_df: rows that failed at least one dimension, with reason flag columns
        result: QDCResult summary object
    """
    expected_dtypes = expected_dtypes or {}
    validity_rules = validity_rules or {}

    completeness_mask = check_completeness(df, required_cols)
    uniqueness_mask = check_uniqueness(df, key_cols)
    consistency_mask = check_consistency(df, expected_dtypes)
    validity_mask = check_validity(df, validity_rules)

    combined_mask = completeness_mask & uniqueness_mask & consistency_mask & validity_mask

    failure_reasons = {
        "completeness": int((~completeness_mask).sum()),
        "uniqueness": int((~uniqueness_mask).sum()),
        "consistency": int((~consistency_mask).sum()),
        "validity": int((~validity_mask).sum()),
    }

    passed_df = df[combined_mask].copy()
    failed_df = df[~combined_mask].copy()

    if not failed_df.empty:
        reason_cols = pd.DataFrame(
            {
                "failed_completeness": ~completeness_mask,
                "failed_uniqueness": ~uniqueness_mask,
                "failed_consistency": ~consistency_mask,
                "failed_validity": ~validity_mask,
            }
        )
        failed_df = failed_df.join(reason_cols.loc[failed_df.index])

    result = QDCResult(
        table_name=table_name,
        total_records=len(df),
        passed_records=len(passed_df),
        failed_records=len(failed_df),
        failure_reasons=failure_reasons,
    )
    logger.info(result.summary())

    return passed_df, failed_df, result