#!/usr/bin/env python3
"""
Alat uji injeksi noise untuk membuktikan mekanisme QDC + fail-fast.

Menginjeksi noise terkontrol ke data/raw/olist_order_items_dataset.csv pada
empat dimensi QDC secara disjoint, lalu mengembalikan data asli bila diperlukan.

Dimensi yang dit Campaign:
  completeness -> product_id dikosongkan          (kolom wajib, bukan key)
  uniqueness   -> duplikasi (order_id, order_item_id)
  consistency  -> price diisi teks non-numerik
  validity     -> price diisi nilai negatif

Contoh:
    python3 scripts/noise_injection.py --rate 2
    python3 scripts/noise_injection.py --restore
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
from datetime import datetime

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(REPO, "data", "raw")
TARGET = "olist_order_items_dataset.csv"
BACKUP_DIR = os.path.join(REPO, "data", "_noise_backup")
MANIFEST = os.path.join(BACKUP_DIR, "manifest.json")

COLUMNS = [
    "order_id", "order_item_id", "product_id", "seller_id",
    "ship_limit_date", "price", "freight_value",
]

DIMENSIONS = ["completeness", "uniqueness", "consistency", "validity"]


def backup() -> None:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    dst = os.path.join(BACKUP_DIR, TARGET)
    if not os.path.exists(dst):
        shutil.copy2(os.path.join(RAW, TARGET), dst)
        with open(MANIFEST, "w") as f:
            json.dump({"backup_at": datetime.now().isoformat(), "file": TARGET}, f)
        print(f"  backup  -> {dst}")
    else:
        print(f"  backup  -> sudah ada (dipakai ulang) {dst}")


def restore() -> None:
    src = os.path.join(BACKUP_DIR, TARGET)
    if not os.path.exists(src):
        print("ERROR: tidak ada backup. Jalankan injeksi dulu.")
        sys.exit(1)
    shutil.copy2(src, os.path.join(RAW, TARGET))
    print(f"  restore -> {TARGET} dikembalikan ke data asli")
    print(f"  (backup masih ada di {BACKUP_DIR}; hapus manual bila sudah selesai)")


def inject(rate_pct: float, seed: int, dims: list[str]) -> dict:
    import random

    path = os.path.join(RAW, TARGET)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = [r for r in reader if r.get("order_id") is not None]
    # gunakan header berkas asli, jangan ditulis manual
    fieldnames = [c for c in fieldnames if c]

    total = len(rows)
    n = int(total * rate_pct / 100)
    if n < len(dims):
        print(f"ERROR: rate {rate_pct}% menghasilkan {n} baris < {len(dims)} dimensi.")
        sys.exit(1)
    per = n // len(dims)

    rng = random.Random(seed)
    idx = rng.sample(range(total), n)
    chosen = {d: idx[i * per:(i + 1) * per] for i, d in enumerate(dims)}

    injected = {d: len(chosen[d]) for d in dims}
    skipped = [d for d in DIMENSIONS if d not in dims]

    for d in chosen:
        for n, i in enumerate(chosen[d]):
            r = rows[i]
            if d == "completeness":
                r["product_id"] = ""                      # -> NaN saat dibaca pandas
            elif d == "consistency":
                # Token TIDAK boleh berada di na_values default pandas
                # ("N/A", "NA", "null", ... akan otomatis jadi NaN dan lolos
                # sebagai missing value, bukan pelanggaran consistency).
                # "INVALID" dan desimal koma "24,90" keduanya memicu kegagalan.
                r["price"] = "INVALID" if n % 2 == 0 else "24,90"
            elif d == "validity":
                r["price"] = "-99.99"                     # negatif
            elif d == "uniqueness":
                # salin kunci baris acuan (baris 0) -> kunci kembar dengan nilai berbeda
                src = rows[0]
                r["order_id"] = src["order_id"]
                r["order_item_id"] = src["order_item_id"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    report = {
        "waktu": datetime.now().isoformat(),
        "file": TARGET,
        "total_baris": total,
        "rate_diminta": rate_pct,
        "dimensi": dims,
        "dimensi_tidak_diuji": skipped,
        "total_noise": sum(injected.values()),
        "per_dimensi": injected,
        "seed": seed,
        "expectation": {
            "fact_sales_failure_rate_pct": round(sum(injected.values()) / total * 100, 3),
            "fail_fast_expected": (sum(injected.values()) / total) > 0.05,
        },
    }
    with open(os.path.join(BACKUP_DIR, "injection_report.json"), "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n  file     : {TARGET}")
    print(f"  baris    : {total:,}")
    print(f"  noise    : {sum(injected.values()):,} baris ({rate_pct}%)")
    for d in dims:
        print(f"    - {d:<14s} {injected[d]:>6,} baris")
    if skipped:
        print(f"  dimensi tidak diuji : {', '.join(skipped)}")
    print(f"\n  ekspektasi failure rate fact_sales : "
          f"{report['expectation']['fact_sales_failure_rate_pct']}%")
    print(f"  ekspektasi fail-fast (>5%)         : "
          f"{'YA' if report['expectation']['fail_fast_expected'] else 'TIDAK'}")
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Injeksi noise untuk uji QDC/fail-fast")
    ap.add_argument("--rate", type=float, help="persentase noise yang disuntikkan (mis. 2 atau 8)")
    ap.add_argument("--seed", type=int, default=42, help="seed agar injeksi reproducible")
    ap.add_argument(
        "--dims",
        default="completeness,uniqueness,consistency,validity",
        help="pisah koma; default menguji keempat dimensi QDC",
    )
    ap.add_argument("--restore", action="store_true", help="kembalikan data/raw ke kondisi asli")
    args = ap.parse_args()

    if args.restore:
        restore()
        return
    if args.rate is None:
        ap.error("wajib menyertakan --rate atau --restore")

    dims = [d.strip() for d in args.dims.split(",") if d.strip()]
    unknown = [d for d in dims if d not in DIMENSIONS]
    if unknown:
        ap.error(f"dimensi tidak dikenal: {unknown}. Pilihan: {DIMENSIONS}")

    if "consistency" in dims:
        print(
            "  CATATAN: dimensi 'consistency' memasukkan teks non-numerik ke price.\n"
            "  check_validity() kini mengoersi kolom object sebelum aturan\n"
            "  diterapkan, sehingga tidak lagi memicu TypeError."
        )

    print("[1/2] backup data asli")
    backup()
    print("[2/2] injeksi noise")
    inject(args.rate, args.seed, dims)


if __name__ == "__main__":
    main()