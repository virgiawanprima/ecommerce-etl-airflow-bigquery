"""Tiga DAG manual untuk mendemonstrasikan QDC tanpa menulis ke dataset utama."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from random import Random

from airflow.sdk import DAG, get_current_context, task

RAW = Path("/opt/airflow/data/raw")
DEMO_ROOT = Path("/opt/airflow/data/processed/presentation_demos")
SCENARIOS = {
    "demo_pipeline": (0, "olist_demo_pipeline", "Data bersih: semua tahap lulus"),
    "demo_noise-2": (2, "olist_demo_noise_2", "Noise 2%: baris valid tetap dimuat"),
    "demo_noise-8": (8, "olist_demo_noise_8", "Noise 8%: QDC menghentikan load"),
}
SOURCES = {
    "orders": ("olist_orders_dataset.csv", ("order_id", "customer_id", "order_status", "order_purchase_timestamp")),
    "order_items": ("olist_order_items_dataset.csv", ("order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value")),
    "customers": ("olist_customers_dataset.csv", ("customer_id", "customer_unique_id", "customer_city", "customer_state")),
    "sellers": ("olist_sellers_dataset.csv", ("seller_id", "seller_city", "seller_state")),
    "products": ("olist_products_dataset.csv", ("product_id", "product_category_name")),
    "reviews": ("olist_order_reviews_dataset.csv", ("review_id", "order_id", "review_score")),
    "payments": ("olist_order_payments_dataset.csv", ("order_id", "payment_type", "payment_value")),
    "geolocation": ("olist_geolocation_dataset.csv", ("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng")),
    "category_translation": ("product_category_name_translation.csv", ("product_category_name", "product_category_name_english")),
}
FACT_COLUMNS = (
    "order_id", "order_item_id", "customer_id", "product_id", "seller_id",
    "order_date", "date_key", "price", "freight_value",
)
AMOUNTS = ("price", "freight_value")
DIMENSION_RULES = {
    "dim_customers": (["customer_id", "customer_city", "customer_state"], ["customer_id"]),
    "dim_products": (["product_id"], ["product_id"]),
    "dim_sellers": (["seller_id"], ["seller_id"]),
    "dim_orders": (["order_id", "order_status"], ["order_id"]),
    "dim_date": (["date_key", "date"], ["date_key"]),
}
logger = logging.getLogger(__name__)


def _workspace() -> Path:
    context = get_current_context()
    dag_id = context["dag"].dag_id
    run_key = sha256(context["run_id"].encode()).hexdigest()[:24]
    return DEMO_ROOT / dag_id / run_key


def _inject_noise(source: Path, destination: Path, rate: int) -> None:
    """Pakai empat kelas anomali dengan sampel deterministik yang tidak tumpang tindih."""
    import csv

    with source.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames
        if not fields or not {"product_id", "order_id", "order_item_id", "price"} <= set(fields):
            raise ValueError("Kolom injeksi noise tidak lengkap.")
        rows = list(reader)

    per_dimension = len(rows) * rate // 400
    if per_dimension == 0:
        raise ValueError("Data terlalu sedikit untuk injeksi keempat kelas anomali.")
    indices = Random(42).sample(range(1, len(rows)), per_dimension * 4)
    reference = rows[0]
    for offset, index in enumerate(indices):
        row = rows[index]
        group = offset // per_dimension
        if group == 0:
            row["product_id"] = ""
        elif group == 1:
            row["order_id"] = reference["order_id"]
            row["order_item_id"] = reference["order_item_id"]
        elif group == 2:
            row["price"] = "INVALID" if offset % 2 == 0 else "24,90"
        else:
            row["price"] = "-99.99"

    with destination.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Noise %s%%: %s dari %s baris, %s per kelas", rate, len(indices), len(rows), per_dimension)


@task
def extract() -> str:
    """Ambil snapshot CSV untuk satu run, tanpa watermark atau simulasi DAG utama."""
    import pandas as pd

    dag_id = get_current_context()["dag"].dag_id
    rate, _, _ = SCENARIOS[dag_id]
    workspace = _workspace()
    if workspace.resolve().is_relative_to(RAW.resolve()) or not workspace.resolve().is_relative_to(DEMO_ROOT):
        raise RuntimeError("Folder staging demo tidak terisolasi.")
    staging = workspace / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    logger.info("Full load demo: noise=%s%%, staging=%s", rate, staging)
    for alias, (filename, _) in SOURCES.items():
        source = RAW / filename
        if alias == "order_items" and rate:
            noise_copy = workspace / filename
            _inject_noise(source, noise_copy, rate)
            source = noise_copy
        frame = pd.read_csv(source)
        frame.to_parquet(staging / f"{alias}.parquet", index=False)
        logger.info("Extract %s: %s baris", alias, len(frame))
    return str(workspace)


@task
def validate_schema(workspace: str) -> str:
    from pyarrow.parquet import read_schema

    for alias, (_, required) in SOURCES.items():
        columns = set(read_schema(Path(workspace) / "staging" / f"{alias}.parquet").names)
        if missing := set(required) - columns:
            raise ValueError(f"Skema {alias}: kolom hilang {sorted(missing)}")
        logger.info("Skema %s lulus: %s kolom", alias, len(columns))
    return workspace


@task
def transform(workspace: str) -> str:
    import pandas as pd

    from src.utils.pii import hash_columns

    root = Path(workspace)
    transformed = root / "transformed"
    transformed.mkdir(exist_ok=True)

    def read(alias: str) -> pd.DataFrame:
        return pd.read_parquet(root / "staging" / f"{alias}.parquet")

    orders = read("orders")
    products = read("products").merge(read("category_translation"), on="product_category_name", how="left")
    tables = {
        "dim_customers": hash_columns(read("customers"), ["customer_unique_id"]),
        "dim_products": products,
        "dim_sellers": read("sellers"),
        "dim_orders": orders,
    }
    dates = pd.to_datetime(orders["order_purchase_timestamp"]).dt.normalize().drop_duplicates()
    dim_date = pd.DataFrame({"date": dates.reset_index(drop=True)})
    dim_date["date_key"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
    for part in ("day", "month", "year", "quarter", "dayofweek"):
        column = "day_of_week" if part == "dayofweek" else part
        dim_date[column] = getattr(dim_date["date"].dt, part)
    dim_date["is_weekend"] = dim_date["day_of_week"].isin([5, 6])
    tables["dim_date"] = dim_date

    fact = read("order_items").merge(
        orders[["order_id", "customer_id", "order_purchase_timestamp"]],
        on="order_id", how="left", validate="many_to_one",
    )
    timestamps = pd.to_datetime(fact["order_purchase_timestamp"])
    fact["order_date"] = timestamps.dt.date
    fact["date_key"] = pd.to_numeric(timestamps.dt.strftime("%Y%m%d"), errors="coerce").astype("Int64")
    tables["fact_sales"] = fact[list(FACT_COLUMNS)]

    for name, frame in tables.items():
        frame.to_parquet(transformed / f"{name}.parquet", index=False)
        logger.info("Transform %s: %s baris", name, len(frame))
    return workspace


@task
def qdc_validation(workspace: str) -> str:
    import pandas as pd

    from src.utils.qdc import FAIL_FAST_THRESHOLD, run_qdc

    def nonnegative(values: pd.Series) -> pd.Series:
        return values >= 0

    root = Path(workspace)
    clean = root / "clean"
    clean.mkdir(exist_ok=True)
    rejected = root / "rejected"
    rejected.mkdir(exist_ok=True)
    failures: list[str] = []
    for name in ("fact_sales", *DIMENSION_RULES):
        frame = pd.read_parquet(root / "transformed" / f"{name}.parquet")
        if name == "fact_sales":
            passed, failed, result = run_qdc(
                frame, name,
                required_cols=["order_id", "order_item_id", "product_id", "seller_id", "date_key", "order_date"],
                key_cols=["order_id", "order_item_id"],
                expected_dtypes={column: "float64" for column in AMOUNTS},
                validity_rules={column: nonnegative for column in AMOUNTS},
            )
            # QDC menguji konversi, tetapi tidak menormalkan tipe pada baris yang lulus.
            for column in AMOUNTS:
                passed[column] = pd.to_numeric(passed[column], errors="raise").astype("float64")
        else:
            required, keys = DIMENSION_RULES[name]
            passed, failed, result = run_qdc(frame, name, required, keys)

        if not failed.empty:
            error_log = rejected / f"{name}_error_log.csv"
            failed.to_csv(error_log, index=False)
            logger.warning("%s: baris ditolak dicatat di %s; alasan=%s", name, error_log, result.failure_reasons)
        passed.to_parquet(clean / f"{name}.parquet", index=False)
        if result.triggers_fail_fast:
            failures.append(result.summary())

    if failures:
        raise ValueError(f"QDC fail-fast (>{FAIL_FAST_THRESHOLD:.0%}); load diblokir: {'; '.join(failures)}")
    return workspace


@task
def load(workspace: str) -> None:
    import pandas as pd
    from google.cloud import bigquery

    dag_id = get_current_context()["dag"].dag_id
    _, dataset_id, _ = SCENARIOS[dag_id]
    protected = {"olist_dw", os.environ.get("BIGQUERY_DATASET", "").strip().lower()}
    if not dataset_id.startswith("olist_demo_") or dataset_id.lower() in protected:
        raise RuntimeError(f"Dataset bukan tujuan demo yang diizinkan: {dataset_id}")

    location = os.environ["BIGQUERY_LOCATION"]
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"], location=location)
    requested = bigquery.Dataset(f"{client.project}.{dataset_id}")
    requested.location = location
    requested.labels = {"purpose": "olist_presentation"}
    requested.default_table_expiration_ms = 7 * 24 * 60 * 60 * 1000
    target = client.create_dataset(requested, exists_ok=True)
    if target.labels.get("purpose") != "olist_presentation" or target.location != location:
        raise RuntimeError(f"Dataset tidak ditandai khusus presentasi atau lokasinya berbeda: {dataset_id}")

    clean = Path(workspace) / "clean"
    for name in (*DIMENSION_RULES, "fact_sales"):
        frame = pd.read_parquet(clean / f"{name}.parquet")
        config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        if name == "fact_sales":
            frame["order_date"] = pd.to_datetime(frame["order_date"]).dt.date
            config.schema = [
                bigquery.SchemaField(column, "FLOAT64") for column in AMOUNTS
            ] + [bigquery.SchemaField("order_date", "DATE")]
        destination = target.reference.table(name)
        job = client.load_table_from_dataframe(frame, destination, job_config=config).result()
        logger.info("Load %s: %s baris, job=%s", destination, job.output_rows, job.job_id)


def _build_demo(dag_id: str, description: str) -> DAG:
    with DAG(
        dag_id=dag_id,
        description=description,
        schedule=None,
        start_date=datetime(2026, 2, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        default_args={"owner": "virgiawan", "retries": 0},
        tags=["demo", "skripsi", "qdc"],
    ) as demo:
        load(qdc_validation(transform(validate_schema(extract()))))
    return demo


demo_pipeline = _build_demo("demo_pipeline", SCENARIOS["demo_pipeline"][2])
demo_noise_2 = _build_demo("demo_noise-2", SCENARIOS["demo_noise-2"][2])
demo_noise_8 = _build_demo("demo_noise-8", SCENARIOS["demo_noise-8"][2])
