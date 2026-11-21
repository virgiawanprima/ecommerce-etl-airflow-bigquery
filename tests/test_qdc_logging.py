import logging

import pandas as pd
import pytest

from src.utils.qdc import run_qdc


@pytest.mark.parametrize(
    ("total", "rejected", "level", "expected_message", "fail_fast"),
    [
        (
            100, 0, logging.INFO,
            "QDC: 0 baris ditolak pada tabel fact_sales (total 100 baris, 0,00%).",
            False,
        ),
        (
            100, 2, logging.WARNING,
            "Peringatan: 2 baris ditolak pada tabel fact_sales (total 100 baris, 2,00%).",
            False,
        ),
        (
            100, 5, logging.WARNING,
            "Peringatan: 5 baris ditolak pada tabel fact_sales (total 100 baris, 5,00%).",
            False,
        ),
        (
            100, 8, logging.WARNING,
            "Peringatan: 8 baris ditolak pada tabel fact_sales (total 100 baris, 8,00%).",
            True,
        ),
        (
            0, 0, logging.INFO,
            "QDC: 0 baris ditolak pada tabel fact_sales (total 0 baris, tidak ada data).",
            False,
        ),
        (
            112650, 2252, logging.WARNING,
            "Peringatan: 2.252 baris ditolak pada tabel fact_sales (total 112.650 baris, 2,00%).",
            False,
        ),
    ],
)
def test_qdc_logs_actual_rejected_rows(
    caplog: pytest.LogCaptureFixture,
    total: int,
    rejected: int,
    level: int,
    expected_message: str,
    fail_fast: bool,
) -> None:
    frame = pd.DataFrame(
        {
            "id": range(total),
            "price": [1.0] * (total - rejected) + [-1.0] * rejected,
        }
    )
    caplog.set_level(logging.INFO, logger="src.utils.qdc")

    passed, failed, result = run_qdc(
        frame,
        "fact_sales",
        required_cols=["id", "price"],
        key_cols=["id"],
        expected_dtypes={"price": "float64"},
        validity_rules={"price": lambda values: values >= 0},
    )

    assert len(passed) == total - rejected
    assert len(failed) == rejected
    assert result.triggers_fail_fast is fail_fast
    assert ("src.utils.qdc", level, expected_message) in caplog.record_tuples
