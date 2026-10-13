"""
DAG: olist_etl_pipeline

Menerapkan pipeline ETL hybrid cloud-native 5 task yang dijelaskan pada
skripsi Bab 3.4.3 dan 3.5.2:

    extract -> validate_schema -> transform -> qdc_validation -> load

Arsitektur:
    - Extract  : membaca 9 CSV Olist; kueri watermark BigQuery untuk mode incremental
    - Validate : memastikan kolom/skema yang diharapkan tersedia pada 9 tabel
    - Transform: membersihkan, menggabungkan ke bentuk star schema, meng-hash kolom PII;
                 menerapkan filter watermark pada fact_sales untuk run incremental
    - QDC      : menjalankan pemeriksaan kualitas 4 dimensi, fail-fast bila gagal >5%
    - Load     : menulis fact_sales + 5 tabel dimensi ke BigQuery (olist_dw)
                 tabel dimensi: WRITE_TRUNCATE (refresh penuh)
                 fact_sales: WRITE_TRUNCATE (run pertama) / WRITE_APPEND (incremental)

Strategi incremental load (Bab 3.4.3):
    - Watermark = MAX(order_date) yang dikueri dari fact_sales sebelum extract
    - Run pertama (tabel kosong / tabel belum ada): full load, WRITE_TRUNCATE
    - Run berikutnya: filter order dengan order_purchase_timestamp::date > watermark
                     lalu hanya append baris fact baru, WRITE_APPEND

Kebijakan retry: 3 kali percobaan, jeda 5 menit, mengacu Bab 3.4.3.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.sdk import dag, task

# Pastikan direktori src/ terdaftar dalam sys.path (kompatibel Docker dan lokal)
_SRC_DIR = str(Path(__file__).resolve().parent.parent / "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

try:
    from src.utils.qdc import run_qdc
    from src.utils.pii import hash_columns
    from src.utils.io_helpers import write_parquet as _write
except ImportError:
    from utils.qdc import run_qdc
    from utils.pii import hash_columns
    from utils.io_helpers import write_parquet as _write

logger = logging.getLogger(__name__)

RAW_DATA_DIR = "/opt/airflow/data/raw"
PROCESSED_DATA_DIR = "/opt/airflow/data/processed"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "gen-lang-client-0814065913")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "olist_dw")
BIGQUERY_LOCATION = os.environ.get("BIGQUERY_LOCATION", "asia-southeast2")

# Peta berkas sumber, mengacu Lampiran 1
CSV_FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

default_args = {
    "owner": "virgiawan",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="olist_etl_pipeline",
    description="ETL hybrid cloud-native: Olist CSV -> star schema -> BigQuery",
    schedule=None,  # dipicu manual saat pengembangan; '@daily' untuk produksi
    start_date=datetime(2026, 2, 1),
    catchup=False,
    default_args=default_args,
    tags=["skripsi", "etl", "olist", "bigquery"],
)
def olist_etl_pipeline():

    @task
    def extract() -> dict[str, str]:
        """Membaca tiap CSV menjadi snapshot parquet di PROCESSED_DATA_DIR.

        Sebelum membaca CSV, melakukan kueri BigQuery untuk memperoleh watermark
        terkini (MAX order_date pada fact_sales). Watermark disimpan sebagai
        metadata key '_watermark' pada dict yang dikembalikan, sehingga task
        hilir dapat menerapkan filter incremental tanpa perlu kueri ulang BQ.
        """
        from google.cloud import bigquery

        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

        # --- Kueri watermark (Bab 3.4.3: logika incremental load) ---
        watermark = None
        try:
            client = bigquery.Client(project=GCP_PROJECT_ID, location=BIGQUERY_LOCATION)
            wm_query = (
                f"SELECT MAX(order_date) AS wm "
                f"FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_sales`"
            )
            for row in client.query(wm_query).result():
                watermark = row.wm  # datetime.date | None
        except Exception as exc:
            # Tabel belum ada (run pertama) atau error BQ -> turun ke mode full load
            logger.info(f"Watermark query skipped ({type(exc).__name__}): full load mode.")
            watermark = None

        load_mode = "incremental" if watermark is not None else "full"
        logger.info(f"Load mode: {load_mode} | watermark: {watermark}")

        # --- Extract seluruh 9 CSV ---
        paths: dict[str, str] = {}
        sim_dir = os.environ.get("OLIST_SIM_DIR", "/opt/airflow/data/sim")
        for alias, filename in CSV_FILES.items():
            src = os.path.join(RAW_DATA_DIR, filename)
            if not os.path.exists(src):
                raise FileNotFoundError(
                    f"Berkas sumber tidak ditemukan: {src}. "
                    f"Taruh CSV Olist di {RAW_DATA_DIR}."
                )
            df = pd.read_csv(src)
            # Simulasi velocity (Bab 3.4.4): ketika OLIST_SIM_DIR memuat CSV
            # dengan nama berkas yang SAMA, baris-barisnya di-append sehingga
            # run berjalan seolah ada data transaksi baru setelah watermark.
            # Nonaktif secara bawaan (direktori kosong/tidak ada) -> run normal
            # tidak terpengaruh.
            sim_file = os.path.join(sim_dir, filename)
            if os.path.exists(sim_file):
                sim_df = pd.read_csv(sim_file)
                df = pd.concat([df, sim_df], ignore_index=True)
                logger.info(
                    f"Velocity simulation: appended {len(sim_df):,} "
                    f"synthetic rows to {alias} from {sim_file}"
                )
            out_path = os.path.join(PROCESSED_DATA_DIR, f"{alias}_extracted.parquet")
            df.to_parquet(out_path, index=False)
            logger.info(f"Extracted {alias}: {len(df):,} rows -> {out_path}")
            paths[alias] = out_path

        # Simpan watermark sebagai metadata untuk task hilir
        paths["_watermark"] = str(watermark) if watermark is not None else ""
        return paths

    @task
    def validate_schema(paths: dict[str, str]) -> dict[str, str]:
        """Memastikan setiap tabel hasil extract memiliki kolom kunci yang diharapkan."""
        # Pisahkan key metadata internal sebelum validasi skema
        meta = {k: v for k, v in paths.items() if k.startswith("_")}
        data_paths = {k: v for k, v in paths.items() if not k.startswith("_")}

        expected_columns = {
            "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
            "order_items": ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
            "customers": ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
            "sellers": ["seller_id", "seller_city", "seller_state"],
            "products": ["product_id", "product_category_name"],
            "reviews": ["review_id", "order_id", "review_score"],
            "payments": ["order_id", "payment_type", "payment_value"],
            "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
            "category_translation": ["product_category_name", "product_category_name_english"],
        }
        for alias, cols in expected_columns.items():
            df = pd.read_parquet(data_paths[alias])
            missing = set(cols) - set(df.columns)
            if missing:
                raise ValueError(
                    f"Validasi skema gagal untuk '{alias}': kolom hilang {missing}"
                )
            logger.info(f"Schema OK: {alias} ({len(df.columns)} kolom)")

        return {**data_paths, **meta}  # pasang kembali metadata

    @task
    def transform(paths: dict[str, str]) -> dict[str, str]:
        """
        Membangun tabel star schema:
          fact_sales, dim_customers, dim_products, dim_sellers, dim_orders, dim_date

        Meng-hash kolom PII (customer_unique_id) mengacu UU PDP.

        Filter incremental (Bab 3.4.3):
          Ketika _watermark terisi, hanya baris order_items yang order-nya
          memiliki order_purchase_timestamp setelah tanggal watermark yang
          dimasukkan ke fact_sales. Tabel dimensi selalu dibangun ulang penuh
          (ukurannya kecil dan mendukung WRITE_TRUNCATE pada tahap load).
        """
        # Ambil metadata
        watermark_str = paths.pop("_watermark", "")
        watermark = None
        if watermark_str:
            import datetime as dt
            try:
                watermark = dt.date.fromisoformat(watermark_str)
            except ValueError:
                watermark = None
        is_incremental = watermark is not None

        out_paths: dict[str, str] = {}

        orders = pd.read_parquet(paths["orders"])
        order_items = pd.read_parquet(paths["order_items"])
        customers = pd.read_parquet(paths["customers"])
        sellers = pd.read_parquet(paths["sellers"])
        products = pd.read_parquet(paths["products"])
        category_translation = pd.read_parquet(paths["category_translation"])

        # --- Tabel dimensi (selalu dibangun ulang penuh) ---

        # dim_customers (PII di-hash mengacu UU PDP)
        dim_customers = hash_columns(customers.copy(), ["customer_unique_id"])
        out_paths["dim_customers"] = _write(dim_customers, "dim_customers")

        # dim_products, dilengkapi nama kategori dalam bahasa Inggris
        dim_products = products.merge(category_translation, on="product_category_name", how="left")
        out_paths["dim_products"] = _write(dim_products, "dim_products")

        # dim_sellers
        out_paths["dim_sellers"] = _write(sellers, "dim_sellers")

        # dim_orders
        out_paths["dim_orders"] = _write(orders, "dim_orders")

        # dim_date: dibangun dari seluruh tanggal pesanan unik pada dataset penuh
        orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])
        dates = orders["order_purchase_timestamp"].dt.date.drop_duplicates().reset_index(drop=True)
        dim_date = pd.DataFrame({"date": pd.to_datetime(dates)})
        dim_date["date_key"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
        dim_date["day"] = dim_date["date"].dt.day
        dim_date["month"] = dim_date["date"].dt.month
        dim_date["year"] = dim_date["date"].dt.year
        dim_date["quarter"] = dim_date["date"].dt.quarter
        dim_date["day_of_week"] = dim_date["date"].dt.dayofweek
        dim_date["is_weekend"] = dim_date["day_of_week"].isin([5, 6])
        out_paths["dim_date"] = _write(dim_date, "dim_date")

        # --- Tabel fakta (sadar-mode incremental) ---
        # Grain = order item; kunci alami (order_id, order_item_id).
        # Produk yang sama dapat muncul beberapa kali dalam satu order
        # (pengiriman massal/terpisah), sehingga product_id saja tidak dapat
        # menjadi bagian dari kunci unik (pelanggaran grain terdeteksi oleh
        # dimensi uniqueness pada QDC - lihat Bab IV).

        orders_ts = orders["order_purchase_timestamp"]  # sudah bertipe datetime

        if is_incremental:
            # Filter: hanya order yang lebih baru dari tanggal watermark
            new_order_mask = orders_ts.dt.date > watermark
            new_order_ids = set(orders.loc[new_order_mask, "order_id"])
            orders_for_fact = orders[new_order_mask].copy()
            items_for_fact = order_items[order_items["order_id"].isin(new_order_ids)].copy()
            logger.info(
                f"Incremental mode: watermark={watermark}, "
                f"new orders={len(orders_for_fact):,}, "
                f"new items={len(items_for_fact):,}"
            )
        else:
            orders_for_fact = orders.copy()
            items_for_fact = order_items.copy()
            logger.info(f"Full load mode: {len(items_for_fact):,} order items")

        fact_sales = items_for_fact.merge(
            orders_for_fact[["order_id", "customer_id", "order_purchase_timestamp"]],
            on="order_id",
            how="left",
        )
        fact_sales["order_purchase_timestamp"] = pd.to_datetime(
            fact_sales["order_purchase_timestamp"]
        )
        # order_date (DATE) - sumber untuk watermark dan analitik berbasis waktu
        fact_sales["order_date"] = fact_sales["order_purchase_timestamp"].dt.date
        fact_sales["date_key"] = (
            fact_sales["order_purchase_timestamp"].dt.strftime("%Y%m%d")
        )
        fact_sales["date_key"] = pd.to_numeric(fact_sales["date_key"], errors="coerce")
        fact_sales = fact_sales[
            [
                "order_id",
                "order_item_id",
                "customer_id",
                "product_id",
                "seller_id",
                "order_date",
                "date_key",
                "price",
                "freight_value",
            ]
        ]
        out_paths["fact_sales"] = _write(fact_sales, "fact_sales")

        # Teruskan flag incremental ke task load
        out_paths["_is_incremental"] = "1" if is_incremental else ""
        return out_paths

    @task
    def qdc_validation(paths: dict[str, str]) -> dict[str, str]:
        """
        Menjalankan QDC 4 dimensi untuk setiap tabel. Fail-fast bila suatu
        tabel melampaui ambang kegagalan 5% (Bab 3.5.1). Baris yang gagal
        ditulis ke CSV error_log untuk diperiksa.
        """
        # Ambil dan teruskan metadata
        meta = {k: v for k, v in paths.items() if k.startswith("_")}
        data_paths = {k: v for k, v in paths.items() if not k.startswith("_")}

        table_configs = {
            "fact_sales": dict(
                required_cols=["order_id", "order_item_id", "product_id", "seller_id", "date_key", "order_date"],
                key_cols=["order_id", "order_item_id"],
                expected_dtypes={"price": "float64", "freight_value": "float64"},
                validity_rules={
                    "price": lambda s: s >= 0,
                    "freight_value": lambda s: s >= 0,
                },
            ),
            "dim_customers": dict(
                required_cols=["customer_id", "customer_city", "customer_state"],
                key_cols=["customer_id"],
            ),
            "dim_products": dict(
                required_cols=["product_id"],
                key_cols=["product_id"],
            ),
            "dim_sellers": dict(
                required_cols=["seller_id", "seller_city", "seller_state"],
                key_cols=["seller_id"],
            ),
            "dim_orders": dict(
                required_cols=["order_id", "order_status"],
                key_cols=["order_id"],
            ),
            "dim_date": dict(
                required_cols=["date_key", "date"],
                key_cols=["date_key"],
            ),
        }

        clean_paths: dict[str, str] = {}
        overall_failures: list[str] = []

        for table_name, cfg in table_configs.items():
            df = pd.read_parquet(data_paths[table_name])
            passed_df, failed_df, result = run_qdc(df, table_name, **cfg)

            if not failed_df.empty:
                error_path = os.path.join(PROCESSED_DATA_DIR, f"{table_name}_error_log.csv")
                failed_df.to_csv(error_path, index=False)
                logger.warning(f"{table_name}: {len(failed_df):,} baris diarahkan ke error_log")

            if result.triggers_fail_fast:
                overall_failures.append(result.summary())

            clean_path = os.path.join(PROCESSED_DATA_DIR, f"{table_name}_clean.parquet")
            passed_df.to_parquet(clean_path, index=False)
            clean_paths[table_name] = clean_path

        if overall_failures:
            raise ValueError(
                "QDC fail-fast triggered (>5% failure rate) for: "
                + "; ".join(overall_failures)
            )

        return {**clean_paths, **meta}  # teruskan metadata ke task load

    @task
    def load(paths: dict[str, str]) -> None:
        """Memuat tabel star schema yang sudah bersih ke BigQuery (asia-southeast2).

        Tabel dimensi: selalu WRITE_TRUNCATE (refresh penuh; ukurannya kecil
        dan harus mencerminkan setiap perubahan SCD dari hulu).

        fact_sales:
          - Run pertama (is_incremental=False): WRITE_TRUNCATE (muat ulang penuh)
          - Run berikutnya (is_incremental=True): WRITE_APPEND (append baris baru)
        """
        from google.cloud import bigquery

        is_incremental = paths.pop("_is_incremental", "") == "1"
        logger.info(
            f"Load mode: {'incremental (WRITE_APPEND)' if is_incremental else 'full (WRITE_TRUNCATE)'}"
        )

        client = bigquery.Client(project=GCP_PROJECT_ID, location=BIGQUERY_LOCATION)
        dataset_ref = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}"

        for table_name, path in paths.items():
            if table_name.startswith("_"):
                continue
            df = pd.read_parquet(path)

            if table_name == "fact_sales":
                # Konversi ke objek date Python (bukan datetime64[ns]) untuk menghindari
                # koersi kolom DATE oleh BigQuery SDK.
                df["order_date"] = pd.to_datetime(df["order_date"]).dt.date

            table_id = f"{dataset_ref}.{table_name}"
            write_disp = (
                bigquery.WriteDisposition.WRITE_APPEND
                if is_incremental and table_name == "fact_sales"
                else bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            job_config = bigquery.LoadJobConfig(write_disposition=write_disp)
            client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
            mode = "APPEND" if is_incremental and table_name == "fact_sales" else "TRUNCATE"
            logger.info(f"Loaded {len(df):,} rows into {table_id} ({mode})")

        # Ringkasan jumlah baris fact_sales untuk observability / evaluasi Bab IV
        fact_table_id = f"{dataset_ref}.fact_sales"
        total = next(
            iter(client.query(f"SELECT COUNT(*) AS n FROM `{fact_table_id}`").result())
        ).n
        logger.info(f"fact_sales now has {total:,} rows ({'APPEND' if is_incremental else 'REPLACE'})")

    # Perakitan task (Bab 3.5.2): extract -> validate_schema -> transform
    # -> qdc_validation -> load. Setiap langkah meneruskan dict keluarannya ke
    # langkah berikutnya melalui argumen XCom TaskFlow. Decorator @task Airflow 3
    # hanya mendaftarkan task ketika fungsi yang didekorasi dipanggil di sini.
    extracted = extract()
    validated = validate_schema(extracted)
    transformed = transform(validated)
    qdc = qdc_validation(transformed)
    load(qdc)


# PENTING: scheduler Airflow 3.x menemukan DAG yang ditugaskan ke variabel
# global tingkat modul. Menugaskan DAG yang telah dibangun ke global bernama
# `dag` merupakan pola pendaftaran idiomatik.
dag = olist_etl_pipeline()
