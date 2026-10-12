"""
Modul Quality Data Control (QDC).

Menerapkan empat dimensi validasi sebagaimana dirancang pada skripsi Bab 2.6:
- completeness : kolom wajib tidak boleh bernilai kosong (null)
- uniqueness   : tidak ada duplikasi pada kunci yang tidak disengaja
- consistency  : tipe data kolom sesuai dengan skema yang diharapkan
- validity     : nilai sesuai dengan logika bisnis (mis. price >= 0)

Setiap pemeriksaan mengembalikan boolean mask (True = lulus) sehingga baris
yang gagal dapat diarahkan ke error_log, bukan sekadar dibuang diam-diam.
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
    """True pada baris yang seluruh kolom wajibnya bernilai non-null."""
    return df[required_cols].notna().all(axis=1)


def check_uniqueness(df: pd.DataFrame, key_cols: list[str]) -> pd.Series:
    """True pada baris yang kombinasi kuncinya tidak duplikat (pertama dipertahankan)."""
    return ~df.duplicated(subset=key_cols, keep="first")


def check_consistency(df: pd.DataFrame, expected_dtypes: dict[str, str]) -> pd.Series:
    """
    True pada baris yang nilainya dapat dikonversi dengan aman ke tipe data
    yang diharapkan. Memakai pemeriksaan koersi per kolom, bukan perbandingan
    tipe data sekaligus, karena kolom CSV mentah umumnya dimuat sebagai
    dtype 'object'.
    """
    mask = pd.Series(True, index=df.index)
    for col, dtype in expected_dtypes.items():
        if col not in df.columns:
            logger.warning(
                "check_consistency: kolom '%s' tidak ada -> aturan dilewati. "
                "Tambahkan ke required_cols agar ketahuan sebagai pelanggaran.",
                col,
            )
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
    True pada baris di mana callable aturan bisnis mengembalikan True untuk
    nilai baris tersebut.
    rules: {nama_kolom: lambda series -> boolean_series}
    Contoh: {"price": lambda s: s >= 0}

    Kolom non-numerik (dtype 'object') dikoersi ke numerik sebelum aturan
    diterapkan. Tanpa langkah ini, nilai seperti "29,90" atau "N/A" membuat
    kolom ber-dtype object dan perbandingan `s >= 0` melempar TypeError,
    sehingga task gagal tanpa menulis error_log. Baris yang tidak dapat
    dikonversi menjadi NaN lalu dianggap gagal (fillna(False)), sehingga
    aturan tetap menangkapnya - hanya tanpa menjatuhkan pipeline.
    """
    mask = pd.Series(True, index=df.index)
    for col, rule_fn in rules.items():
        if col not in df.columns:
            logger.warning(
                "check_validity: kolom '%s' tidak ada -> aturan dilewati. "
                "Periksa konfigurasi QDC untuk tabel ini.",
                col,
            )
            continue
        series = df[col]
        try:
            outcome = rule_fn(series)
        except TypeError:
            coerced = pd.to_numeric(series, errors="coerce")
            unconvertible = int(series.notna().sum() - coerced.notna().sum())
            logger.info(
                "check_validity: kolom '%s' dikoersi ke numerik (%d nilai "
                "tidak dapat dikonversi) sebelum aturan diterapkan.",
                col,
                unconvertible,
            )
            outcome = rule_fn(coerced)
        mask &= outcome.fillna(False)
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
    Menjalankan keempat dimensi QDC lalu membagi dataframe menjadi
    (lulus, gagal).

    Mengembalikan:
        passed_df: baris yang memenuhi seluruh dimensi
        failed_df: baris yang gagal pada minimal satu dimensi, dilengkapi
                   kolom penanda alasan kegagalan
        result: objek ringkasan QDCResult
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