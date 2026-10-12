"""
Uji mandiri (berbasis assert) untuk logika non-trivial pipeline.

Jalankan di dalam container Airflow (memiliki dependensi pandas dan utilitas proyek):

    docker compose exec airflow-scheduler python /opt/airflow/src/utils/self_check.py

Memverifikasi mekanisme QDC (4 dimensi dan ambang batas fail-fast) serta hashing PII (UU PDP)
tanpa memerlukan BigQuery atau orkestrasi penuh Airflow.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from qdc import check_validity, run_qdc  # noqa: E402
from pii import hash_columns  # noqa: E402

PASS = 0


def check(name: str, cond: bool) -> None:
    global PASS
    assert cond, f"FAIL: {name}"
    PASS += 1
    print(f"  ok: {name}")


# ---- QDC: completeness / uniqueness / consistency / validity ----
df = pd.DataFrame(
    {
        "order_id": ["o1", "o1", "o2", "o3", None],
        "order_item_id": [1, 2, 1, 1, 1],
        "price": [10.5, 20.0, None, -5.0, 7.0],
        "freight_value": [3.0, 4.0, 5.0, 6.0, 7.0],
    }
)

passed, failed, result = run_qdc(
    df,
    "fact_sales",
    required_cols=["order_id", "order_item_id"],
    key_cols=["order_id", "order_item_id"],
    expected_dtypes={"price": "float64", "freight_value": "float64"},
    validity_rules={"price": lambda s: s >= 0},
)

check("total records = 5", result.total_records == 5)
check("passed records = 2", result.passed_records == 2)  # row0, row1 pass
check("failed records = 3", result.failed_records == 3)  # dup-key, null price, negative price, null key
check("success_rate = 0.4", abs(result.success_rate - 0.4) < 1e-9)
check("triggers fail-fast (>5%)", result.triggers_fail_fast is True)
check("meets_target (>=95%) is False", result.meets_target is False)
check("failed rows have reason columns", {"failed_completeness", "failed_uniqueness", "failed_consistency", "failed_validity"}.issubset(failed.columns))

# ---- QDC: data bersih lolos 100% ----
clean = pd.DataFrame(
    {
        "order_id": ["o1", "o2"],
        "order_item_id": [1, 1],
        "price": [10.0, 20.0],
        "freight_value": [1.0, 2.0],
    }
)
_, _, res_clean = run_qdc(
    clean, "fact_sales",
    required_cols=["order_id", "order_item_id"],
    key_cols=["order_id", "order_item_id"],
    expected_dtypes={"price": "float64"},
    validity_rules={"price": lambda s: s >= 0},
)
check("clean data: 100% pass", res_clean.success_rate == 1.0 and res_clean.triggers_fail_fast is False)

# ---- PII: hash SHA-256 ----
customers = pd.DataFrame(
    {
        "customer_id": ["c1", "c2"],
        "customer_unique_id": ["alice@example.com", "bob@example.com"],
        "customer_city": ["Sao Paulo", "Rio"],
    }
)
hashed = hash_columns(customers.copy(), ["customer_unique_id"])
expected_hash = hashlib.sha256("alice@example.com".encode("utf-8")).hexdigest()
check("PII column is hashed", hashed.loc[0, "customer_unique_id"] == expected_hash)
check("PII hash is 64 hex chars", len(hashed.loc[1, "customer_unique_id"]) == 64)
check("non-PII column unchanged", hashed.loc[0, "customer_city"] == "Sao Paulo")
check("original df not mutated", customers.loc[0, "customer_unique_id"] == "alice@example.com")

# ---- QDC: regresi kolom non-numerik pada check_validity ----
# Bug lama: kolom 'price' ber-dtype object membuat `s >= 0` melempar
# TypeError, sehingga task gagal tanpa menulis error_log. check_validity
# kini mengoersi kolom object ke numerik sebelum aturan diterapkan.
mixed = pd.DataFrame(
    {
        "price": [10.0, "N/A", 20.0, -5.0],
        "freight_value": [1.0, 2.0, 3.0, 4.0],
    }
)
try:
    mask = check_validity(mixed, {"price": lambda s: s >= 0})
    check("object dtype tidak lagi crash", True)
    check("nilai non-numerik dianggap gagal", mask.tolist() == [True, False, True, False])
except TypeError as exc:  # pragma: no cover - hanya jalan bila bug kembali
    check(f"object dtype tidak lagi crash ({exc})", False)

# Desimal koma ala Brazil ("24,90") juga harus tertangani, bukan crash.
brl = pd.DataFrame({"price": ["24,90", "10.0", "7,50"]})
mask_brl = check_validity(brl, {"price": lambda s: s >= 0})
check("desimal koma tertangani tanpa crash", mask_brl.tolist() == [False, True, False])

# Aturan non-numerik harus tetap berfungsi (tidak di-koersi paksa).
status = pd.DataFrame({"order_status": ["created", "shipped", "canceled"]})
mask_status = check_validity(status, {"order_status": lambda s: s.isin(["created", "shipped"])})
check("aturan non-numerik tetap berfungsi", mask_status.tolist() == [True, True, False])

# Data numerik bersih tidak boleh terpengaruh oleh perubahan ini.
clean_num = pd.DataFrame({"price": [10.0, 20.0, 30.0], "freight_value": [1.0, 2.0, 3.0]})
mask_clean = check_validity(clean_num, {"price": lambda s: s >= 0, "freight_value": lambda s: s >= 0})
check("data numerik bersih tetap 100% lulus", mask_clean.tolist() == [True, True, True])

# ---- QDC: keempat dimensi pada data dengan noise campuran ----
_, failed_mixed, res_mixed = run_qdc(
    mixed, "fact_sales",
    required_cols=["price", "freight_value"],
    key_cols=["price"],
    expected_dtypes={"price": "float64", "freight_value": "float64"},
    validity_rules={"price": lambda s: s >= 0, "freight_value": lambda s: s >= 0},
)
check("run_qdc selesai tanpa crash pada kolom object", res_mixed.total_records == 4)
check(
    "baris non-numerik masuk error_log",
    set(res_mixed.failure_reasons) == {"completeness", "uniqueness", "consistency", "validity"},
)

print(f"\nALL {PASS} CHECKS PASSED")
