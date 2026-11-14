"""Bantuan IO kecil yang dipakai bersama oleh seluruh task DAG."""
from __future__ import annotations

import os

import pandas as pd

PROCESSED_DATA_DIR = "/opt/airflow/data/processed"
STAGING_DATA_DIR = os.path.join(PROCESSED_DATA_DIR, "staging")
CLEAN_DATA_DIR = os.path.join(PROCESSED_DATA_DIR, "clean")
REJECTED_DATA_DIR = os.path.join(PROCESSED_DATA_DIR, "rejected")


def write_parquet(df: pd.DataFrame, name: str, subdir: str = "staging") -> str:
    target_dir = os.path.join(PROCESSED_DATA_DIR, subdir) if subdir else PROCESSED_DATA_DIR
    os.makedirs(target_dir, exist_ok=True)
    path = os.path.join(target_dir, f"{name}.parquet")
    df.to_parquet(path, index=False)
    return path