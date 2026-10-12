"""
Perlindungan PII melalui hashing SHA-256, mengacu pada skripsi Bab 3.4.3
dan ketentuan UU PDP.

Diterapkan pada kolom yang dapat mengidentifikasi individu, misalnya
customer_unique_id. Hashing bersifat satu arah dan deterministik: input
yang sama selalu menghasilkan hash yang sama, sehingga kemampuan join dan
pengelompokan di BigQuery tetap terjaga tanpa mengekspos identifier asli.
"""
from __future__ import annotations

import hashlib

import pandas as pd


def hash_value(value: str, salt: str = "") -> str:
    """Hash SHA-256 dari satu nilai. Nilai kosong/NaN diteruskan tanpa di-hash."""
    if pd.isna(value) or value == "":
        return value
    return hashlib.sha256(f"{salt}{value}".encode("utf-8")).hexdigest()


def hash_columns(df: pd.DataFrame, columns: list[str], salt: str = "") -> pd.DataFrame:
    """Mengembalikan salinan df dengan kolom yang ditentukan sudah di-hash SHA-256."""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: hash_value(v, salt))
    return df