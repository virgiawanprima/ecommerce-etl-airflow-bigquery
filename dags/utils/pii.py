"""
PII protection via SHA-256 hashing, per skripsi Bab 3.4.3 and UU PDP compliance.

Applied to columns that could identify an individual (e.g. customer_unique_id).
Hashing is one-way and deterministic: same input always produces the same
hash, which preserves join/grouping capability in BigQuery without exposing
the raw identifier.
"""
from __future__ import annotations

import hashlib

import pandas as pd


def hash_value(value: str, salt: str = "") -> str:
    """SHA-256 hash of a single value. Empty/NaN values pass through unhashed."""
    if pd.isna(value) or value == "":
        return value
    return hashlib.sha256(f"{salt}{value}".encode("utf-8")).hexdigest()


def hash_columns(df: pd.DataFrame, columns: list[str], salt: str = "") -> pd.DataFrame:
    """Returns a copy of df with the given columns SHA-256 hashed in place."""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: hash_value(v, salt))
    return df