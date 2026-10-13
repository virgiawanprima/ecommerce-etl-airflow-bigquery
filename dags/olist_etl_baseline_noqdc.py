"""
DAG baseline: olist_etl_baseline_noqdc

Variasi dari `olist_etl_pipeline` dengan SATU perbedaan: task
`qdc_validation` dihilangkan.extract -> validate_schema -> transform -> load

Tujuan: mengukur selisih durasi pipeline dengan dan tanpa gerbang QDC,
sehingga biaya quality control terukur secara empiris (bukan sekadar klaim).
Semua logika task lain identik dengan DAG utama agar perbandingan fair.

Penggunaan:
    airflow dags trigger olist_etl_baseline_noqdc

CATATAN: run ini menulis fact_sales TANPA cleaning QDC. Setelah selesai,
jalankan ulang `olist_etl_pipeline` untuk mengembalikan data bersih.
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
    from src.utils.io_helpers import write_parquet as _write
    from src.utils.pii import hash_columns
except ImportError:
    from utils.io_helpers import write_parquet as _write
    from utils.pii import hash_columns

logger = logging.getLogger(__name__)

RAW_DATA_DIR = "/opt/airflow/data/raw"
PROCESSED_DATA_DIR = "/opt/airflow/data/processed"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "gen-lang-client-0814065913")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "olist_dw")
BIGQUERY_LOCATION = os.environ.get("BIGQUERY_LOCATION", "asia-southeast2")

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
    dag_id="olist_etl_baseline_noqdc",
    description="BASELINE (tanpa QDC) - pembanding durasi vs olist_etl_pipeline",
    schedule=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    default_args=default_args,
    tags=["baseline", "no-qdc", "olist", "bigquery"],
)
def olist_etl_baseline_noqdc():

    @task
    def extract() -> dict[str, str]:
        """Sama persis dengan DAG utama."""
        from google.cloud import bigquery

        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

        watermark = None
        try:
            client = bigquery.Client(project=GCP_PROJECT_ID, location=BIGQUERY_LOCATION)
            wm_query = (
                f"SELECT MAX(order_date) AS wm "
                f"FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_sales`"
            )
            for row in client.query(wm_query).result():
                watermark = row.wm
        except Exception as exc:
            logger.info(f"Watermark query skipped ({type(exc).__name__}): full load mode.")
            watermark = None

        load_mode = "incremental" if watermark is not None else "full"
        logger.info(f"Load mode: {load_mode} | watermark: {watermark}")

        paths: dict[str, str] = {}
        for alias, filename in CSV_FILES.items():
            src = os.path.join(RAW_DATA_DIR, filename)
            if not os.path.exists(src):
                raise FileNotFoundError(f"Missing source file: {src}")
            df = pd.read_csv(src)
            out_path = os.path.join(PROCESSED_DATA_DIR, f"{alias}_extracted.parquet")
            df.to_parquet(out_path, index=False)
            logger.info(f"Extracted {alias}: {len(df):,} rows -> {out_path}")
            paths[alias] = out_path

        paths["_watermark"] = str(watermark) if watermark is not None else ""
        return paths

    @task
    def validate_schema(paths: dict[str, str]) -> dict[str, str]:
        """Sama persis dengan DAG utama."""
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
                    f"Schema validation failed for '{alias}': missing columns {missing}"
                )
            logger.info(f"Schema OK: {alias} ({len(df.columns)} columns)")
        return {**data_paths, **meta}

    @task
    def transform(paths: dict[str, str]) -> dict[str, str]:
        """Sama persis dengan DAG utama; tanpa cleaning QDC seperti pada aslinya."""
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

        out_paths["dim_customers"] = _write(
            hash_columns(customers.copy(), ["customer_unique_id"]), "dim_customers"
        )
        out_paths["dim_products"] = _write(
            products.merge(category_translation, on="product_category_name", how="left"),
            "dim_products",
        )
        out_paths["dim_sellers"] = _write(sellers, "dim_sellers")
        out_paths["dim_orders"] = _write(orders, "dim_orders")

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

        orders_ts = orders["order_purchase_timestamp"]
        if is_incremental:
            new_order_mask = orders_ts.dt.date > watermark
            new_order_ids = set(orders.loc[new_order_mask, "order_id"])
            orders_for_fact = orders[new_order_mask].copy()
            items_for_fact = order_items[order_items["order_id"].isin(new_order_ids)].copy()
            logger.info(
                f"Incremental mode: watermark={watermark}, "
                f"new orders={len(orders_for_fact):,}, new items={len(items_for_fact):,}"
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
        fact_sales["order_date"] = fact_sales["order_purchase_timestamp"].dt.date
        fact_sales["date_key"] = fact_sales["order_purchase_timestamp"].dt.strftime("%Y%m%d")
        fact_sales["date_key"] = pd.to_numeric(fact_sales["date_key"], errors="coerce")
        fact_sales = fact_sales[[
            "order_id", "order_item_id", "customer_id", "product_id", "seller_id",
            "order_date", "date_key", "price", "freight_value",
        ]]
        out_paths["fact_sales"] = _write(fact_sales, "fact_sales")
        out_paths["_is_incremental"] = "1" if is_incremental else ""
        return out_paths

    @task
    def load(paths: dict[str, str]) -> None:
        """Sama persis dengan DAG utama; menulis data TANPA cleaning QDC."""
        from google.cloud import bigquery

        is_incremental = paths.pop("_is_incremental", "") == "1"
        logger.info(
            f"BASELINE load mode: {'incremental (WRITE_APPEND)' if is_incremental else 'full (WRITE_TRUNCATE)'}"
        )

        client = bigquery.Client(project=GCP_PROJECT_ID, location=BIGQUERY_LOCATION)
        dataset_ref = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}"

        for table_name, path in paths.items():
            if table_name.startswith("_"):
                continue
            df = pd.read_parquet(path)
            if table_name == "fact_sales":
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
            logger.info(f"Loaded {len(df):,} rows into {table_id} ({mode}) [BASELINE - tanpa QDC]")

        fact_table_id = f"{dataset_ref}.fact_sales"
        total = next(
            iter(client.query(f"SELECT COUNT(*) AS n FROM `{fact_table_id}`").result())
        ).n
        logger.info(f"fact_sales now has {total:,} rows ({'APPEND' if is_incremental else 'REPLACE'})")

    # Perakitan task tanpa qdc_validation
    extracted = extract()
    validated = validate_schema(extracted)
    transformed = transform(validated)
    load(transformed)


dag = olist_etl_baseline_noqdc()