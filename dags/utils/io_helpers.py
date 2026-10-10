"""Bantuan IO kecil yang dipakai bersama oleh seluruh task DAG."""
from __future__ import annotations

import os

import pandas as pd

PROCESSED_DATA_DIR = "/opt/airflow/data/processed"


def write_parquet(df: pd.DataFrame, name: str) -> str:
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    path = os.path.join(PROCESSED_DATA_DIR, f"{name}.parquet")
    df.to_parquet(path, index=False)
    return path