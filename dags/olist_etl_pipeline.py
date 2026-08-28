"""
DAG: olist_etl_pipeline

Implements the 5-task hybrid cloud-native ETL pipeline described in
skripsi Bab 3.4.3 and 3.5.2:

    extract -> validate_schema -> transform -> qdc_validation -> load

Architecture:
    - Extract  : read 9 Olist CSVs; query BigQuery watermark for incremental mode
    - Validate : confirm expected columns/schema are present for all 9 tables
    - Transform: clean, join into star schema shape, hash PII columns;
                 apply watermark filter to fact_sales for incremental runs
    - QDC      : run 4-dimension quality checks, fail-fast at >5% failure
    - Load     : write fact_sales + 5 dim tables to BigQuery (olist_dw)
                 dim tables: WRITE_TRUNCATE (full refresh)
                 fact_sales: WRITE_TRUNCATE (first run) / WRITE_APPEND (incremental)

Incremental load strategy (Bab 3.4.3):
    - Watermark = MAX(order_date) queried from fact_sales before extract
    - First run (empty table / table absent): full load, WRITE_TRUNCATE
    - Subsequent runs: filter orders where order_purchase_timestamp::date > watermark
                       then append only new fact rows, WRITE_APPEND

Retry policy: 3 retries, 5 minute delay, per Bab 3.4.3.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.sdk import dag, task

from utils.qdc import run_qdc
from utils.pii import hash_columns
from utils.io_helpers import write_parquet as _write

logger = logging.getLogger(__name__)

RAW_DATA_DIR = "/opt/airflow/data/raw"
PROCESSED_DATA_DIR = "/opt/airflow/data/processed"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "gen-lang-client-0814065913")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "olist_dw")
BIGQUERY_LOCATION = os.environ.get("BIGQUERY_LOCATION", "asia-southeast2")

# Source file map, per Lampiran 1
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
    description="Hybrid cloud-native ETL: Olist CSV -> star schema -> BigQuery",
    schedule=None,  # trigger manually during dev; '@daily' for production
    start_date=datetime(2026, 2, 1),
    catchup=False,
    default_args=default_args,
    tags=["skripsi", "etl", "olist", "bigquery"],
)
def olist_etl_pipeline():

    @task
    def extract() -> dict[str, str]:
        """Read each CSV into a parquet snapshot in PROCESSED_DATA_DIR.

        Before reading CSVs, queries BigQuery for the current watermark
        (MAX order_date in fact_sales). The watermark is stored as a
        metadata key '_watermark' in the returned dict so downstream
        tasks can apply incremental filtering without re-querying BQ.
        """
        from google.cloud import bigquery

        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

        # --- Watermark query (Bab 3.4.3: incremental load logic) ---
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
            # Table absent (first run) or any BQ error -> fall back to full load
            logger.info(f"Watermark query skipped ({type(exc).__name__}): full load mode.")
            watermark = None

        load_mode = "incremental" if watermark is not None else "full"
        logger.info(f"Load mode: {load_mode} | watermark: {watermark}")

        # --- Extract all 9 CSVs ---
        paths: dict[str, str] = {}
        sim_dir = os.environ.get("OLIST_SIM_DIR", "/opt/airflow/data/sim")
        for alias, filename in CSV_FILES.items():
            src = os.path.join(RAW_DATA_DIR, filename)
            if not os.path.exists(src):
                raise FileNotFoundError(
                    f"Missing source file: {src}. "
                    f"Place the Olist CSVs in {RAW_DATA_DIR}."
                )
            df = pd.read_csv(src)
            # Velocity simulation (Bab 3.4.4): when OLIST_SIM_DIR contains a
            # CSV with the SAME filename, its rows are appended so the run
            # behaves as if new transaction data arrived after the watermark.
            # Disabled by default (dir empty/absent) -> normal runs unaffected.
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

        # Store watermark as metadata for downstream tasks
        paths["_watermark"] = str(watermark) if watermark is not None else ""
        return paths

    @task
    def validate_schema(paths: dict[str, str]) -> dict[str, str]:
        """Confirm each extracted table has its expected key columns present."""
        # Strip internal metadata key before schema validation
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

        return {**data_paths, **meta}  # re-attach metadata

    @task
    def transform(paths: dict[str, str]) -> dict[str, str]:
        """
        Build star schema tables:
          fact_sales, dim_customers, dim_products, dim_sellers, dim_orders, dim_date

        Hashes PII columns (customer_unique_id) per UU PDP.

        Incremental filtering (Bab 3.4.3):
          When _watermark is set, only order_items rows whose order has an
          order_purchase_timestamp strictly after the watermark date are
          included in fact_sales. Dimension tables are always fully rebuilt
          (they are small and support WRITE_TRUNCATE in the load step).
        """
        # Extract metadata
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

        # --- Dimension tables (always full rebuild) ---

        # dim_customers (PII hashed per UU PDP)
        dim_customers = hash_columns(customers.copy(), ["customer_unique_id"])
        out_paths["dim_customers"] = _write(dim_customers, "dim_customers")

        # dim_products, enriched with English category name
        dim_products = products.merge(category_translation, on="product_category_name", how="left")
        out_paths["dim_products"] = _write(dim_products, "dim_products")

        # dim_sellers
        out_paths["dim_sellers"] = _write(sellers, "dim_sellers")

        # dim_orders
        out_paths["dim_orders"] = _write(orders, "dim_orders")

        # dim_date: generated from all unique order dates in the full dataset
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

        # --- Fact table (incremental-aware) ---
        # Grain = order item; natural key (order_id, order_item_id).
        # The same product may appear multiple times in one order (bulk/split
        # shipments), so product_id alone cannot form part of the unique key
        # (grain violation detected by QDC uniqueness dimension - see Bab IV).

        orders_ts = orders["order_purchase_timestamp"]  # already datetime

        if is_incremental:
            # Filter: keep only orders strictly newer than the watermark date
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
        # order_date (DATE) - source for watermark and time-based analytics
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

        # Pass incremental flag to load task
        out_paths["_is_incremental"] = "1" if is_incremental else ""
        return out_paths

    @task
    def qdc_validation(paths: dict[str, str]) -> dict[str, str]:
        """
        Run 4-dimension QDC per table. Fail-fast if any table exceeds the
        5% failure threshold (Bab 3.5.1). Failed rows are written to
        error_log CSVs for inspection.
        """
        # Extract and pass through metadata
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
                logger.warning(f"{table_name}: {len(failed_df):,} rows routed to error_log")

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

        return {**clean_paths, **meta}  # forward metadata to load

    @task
    def load(paths: dict[str, str]) -> None:
        """Load clean star-schema tables into BigQuery (asia-southeast2).

        Dimension tables: always WRITE_TRUNCATE (full refresh; they are small
        and must reflect any upstream SCD changes).

        fact_sales:
          - First run  (is_incremental=False): WRITE_TRUNCATE (full reload)
          - Subsequent (is_incremental=True):  WRITE_APPEND (append new rows)
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
                # Convert to Python date objects (not datetime64[ns]) to avoid
                # the BigQuery SDK coercion of the DATE column.
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

        # Summary row-count of fact_sales for observability / Bab IV evaluation
        fact_table_id = f"{dataset_ref}.fact_sales"
        total = next(
            iter(client.query(f"SELECT COUNT(*) AS n FROM `{fact_table_id}`").result())
        ).n
        logger.info(f"fact_sales now has {total:,} rows ({'APPEND' if is_incremental else 'REPLACE'})")

    # Task wiring (Bab 3.5.2): extract -> validate_schema -> transform
    # -> qdc_validation -> load. Each step passes its output dict to the next
    # via TaskFlow XCom arguments. Airflow 3 @task decorators register a task
    # only when the decorated function is called here.
    extracted = extract()
    validated = validate_schema(extracted)
    transformed = transform(validated)
    qdc = qdc_validation(transformed)
    load(qdc)


# IMPORTANT: Airflow 3.x scheduler discovers DAGs assigned to module-level
# global variables. Assigning the constructed DAG to a global named `dag`
# is the idiomatic registration pattern.
dag = olist_etl_pipeline()
