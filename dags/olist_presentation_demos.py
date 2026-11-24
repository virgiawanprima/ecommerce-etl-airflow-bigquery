"""DAG manual untuk mendemonstrasikan QDC dan incremental load di dataset demo."""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from random import Random

from airflow.sdk import DAG, get_current_context, task

RAW = Path("/opt/airflow/data/raw")
DEMO_ROOT = Path("/opt/airflow/data/processed/presentation_demos")
INCREMENTAL_DEMO = "demo_incremental"
SCENARIOS = {
    "demo_pipeline": (0, "olist_demo_pipeline", "Data bersih: semua tahap lulus"),
    "demo_noise-2": (2, "olist_demo_noise_2", "Noise 2%: baris valid tetap dimuat"),
    "demo_noise-8": (8, "olist_demo_noise_8", "Noise 8%: QDC menghentikan load"),
    INCREMENTAL_DEMO: (0, "olist_demo_incremental", "Full load awal, lalu append pesanan setelah watermark"),
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


def _demo_dataset(dag_id: str) -> str:
    _, dataset_id, _ = SCENARIOS[dag_id]
    protected = {"olist_dw", os.environ.get("BIGQUERY_DATASET", "").strip().lower()}
    if not dataset_id.startswith("olist_demo_") or dataset_id.lower() in protected:
        raise RuntimeError(f"Dataset bukan tujuan demo yang diizinkan: {dataset_id}")
    return dataset_id


def _query_watermark(dataset_id: str) -> date | None:
    from google.api_core.exceptions import Forbidden, NotFound
    from google.cloud import bigquery

    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"], location=os.environ["BIGQUERY_LOCATION"])
    table = f"{client.project}.{dataset_id}.fact_sales"
    try:
        watermark = next(iter(client.query(f"SELECT MAX(order_date) AS wm FROM `{table}`").result())).wm
    except NotFound:
        logger.info("Watermark %s: tabel atau dataset belum tersedia; mode=FULL", table)
        return None
    except Forbidden:
        logger.error("Watermark %s: akses ditolak; extract dihentikan, tidak beralih ke FULL", table)
        raise
    if watermark is None:
        logger.info("Watermark %s: MAX(order_date) kosong; mode=FULL", table)
        return None
    logger.info("Watermark %s: MAX(order_date)=%s; mode=INCREMENTAL", table, watermark)
    return watermark


def _read_watermark(workspace: Path) -> date | None:
    value = (workspace / "watermark.txt").read_text(encoding="utf-8").strip()
    return date.fromisoformat(value) if value else None


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
    """Ambil snapshot CSV dan catat watermark untuk skenario incremental."""
    import pandas as pd

    context = get_current_context()
    dag_id = context["dag"].dag_id
    rate, _, description = SCENARIOS[dag_id]
    dataset_id = _demo_dataset(dag_id)
    workspace = _workspace()
    if workspace.resolve().is_relative_to(RAW.resolve()) or not workspace.resolve().is_relative_to(DEMO_ROOT):
        raise RuntimeError("Folder staging demo tidak terisolasi.")
    staging = workspace / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Extract demo %s: run_id=%s; dataset=%s; skenario=%s; noise target=%s%%; staging=%s",
        dag_id, context["run_id"], dataset_id, description, rate, staging,
    )
    if dag_id == INCREMENTAL_DEMO:
        watermark = _query_watermark(dataset_id)
        (workspace / "watermark.txt").write_text(watermark.isoformat() if watermark else "", encoding="utf-8")
    else:
        logger.info("Mode demo %s: FULL; snapshot penuh tanpa kueri watermark", dag_id)
    for alias, (filename, _) in SOURCES.items():
        source = RAW / filename
        if alias == "order_items" and rate:
            noise_copy = workspace / filename
            _inject_noise(source, noise_copy, rate)
            source = noise_copy
        frame = pd.read_csv(source)
        frame.to_parquet(staging / f"{alias}.parquet", index=False)
        logger.info(
            "Extract %s: sumber=%s; baris=%s; salinan noise=%s",
            alias, filename, len(frame), "ya" if source != RAW / filename else "tidak",
        )
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
    if get_current_context()["dag"].dag_id == INCREMENTAL_DEMO:
        source_rows = len(fact)
        watermark = _read_watermark(root)
        if watermark is None:
            purchase_dates = sorted(fact["order_date"].dropna().unique())
            if len(purchase_dates) < 2:
                raise ValueError("Demo incremental membutuhkan transaksi pada minimal dua tanggal.")
            cutoff = purchase_dates[(len(purchase_dates) - 1) // 2]
            fact = fact.loc[(timestamps.dt.normalize() <= pd.Timestamp(cutoff)) | timestamps.isna()]
            logger.info(
                "Transform fact_sales: mode=FULL; tanggal batch awal <= %s; "
                "kandidat QDC=%s/%s; baris disisihkan untuk run berikutnya=%s",
                cutoff, len(fact), source_rows, source_rows - len(fact),
            )
        else:
            fact = fact.loc[timestamps.dt.normalize() > pd.Timestamp(watermark)]
            logger.info(
                "Transform fact_sales: mode=INCREMENTAL; order_date > %s; "
                "kandidat QDC=%s/%s; baris di luar filter=%s",
                watermark, len(fact), source_rows, source_rows - len(fact),
            )
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
    tables_with_rejections = 0
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
            tables_with_rejections += 1
            error_log = rejected / f"{name}_error_log.csv"
            failed.to_csv(error_log, index=False)
            logger.warning("%s: baris ditolak dicatat di %s; alasan=%s", name, error_log, result.failure_reasons)
        passed.to_parquet(clean / f"{name}.parquet", index=False)
        if result.triggers_fail_fast:
            failures.append(result.summary())

    if failures:
        raise ValueError(f"QDC fail-fast (>{FAIL_FAST_THRESHOLD:.0%}); load diblokir: {'; '.join(failures)}")
    logger.info(
        "QDC selesai: %s tabel diperiksa; %s tabel dengan penolakan; "
        "tidak ada tabel yang melampaui ambang %s; load diizinkan",
        1 + len(DIMENSION_RULES), tables_with_rejections, f"{FAIL_FAST_THRESHOLD:.0%}",
    )
    return workspace


@task
def load(workspace: str) -> None:
    import pandas as pd
    from google.api_core.exceptions import Conflict
    from google.cloud import bigquery

    dag_id = get_current_context()["dag"].dag_id
    dataset_id = _demo_dataset(dag_id)
    clean = Path(workspace) / "clean"
    watermark = _read_watermark(Path(workspace)) if dag_id == INCREMENTAL_DEMO else None
    mode = "INCREMENTAL" if watermark is not None else "FULL"
    logger.info("Load demo %s: dataset=%s; mode=%s; watermark=%s", dag_id, dataset_id, mode, watermark)
    fact = pd.read_parquet(clean / "fact_sales.parquet") if watermark is not None else None
    if fact is not None and fact.empty:
        logger.info(
            "Load %s.fact_sales: mode=INCREMENTAL; watermark=%s; 0 baris lolos QDC; "
            "pemuatan dilewati untuk semua tabel",
            dataset_id, watermark,
        )
        return

    location = os.environ["BIGQUERY_LOCATION"]
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"], location=location)
    requested = bigquery.Dataset(f"{client.project}.{dataset_id}")
    requested.location = location
    requested.labels = {"purpose": "olist_presentation"}
    requested.default_table_expiration_ms = 60 * 24 * 60 * 60 * 1000
    target = client.create_dataset(requested, exists_ok=True)
    if target.labels.get("purpose") != "olist_presentation" or target.location != location:
        raise RuntimeError(f"Dataset tidak ditandai khusus presentasi atau lokasinya berbeda: {dataset_id}")

    total_written = 0
    for name in (*DIMENSION_RULES, "fact_sales"):
        append = name == "fact_sales" and watermark is not None
        frame = fact if append else pd.read_parquet(clean / f"{name}.parquet")
        disposition = bigquery.WriteDisposition.WRITE_APPEND if append else bigquery.WriteDisposition.WRITE_TRUNCATE
        config = bigquery.LoadJobConfig(write_disposition=disposition)
        if name == "fact_sales":
            frame["order_date"] = pd.to_datetime(frame["order_date"]).dt.date
            config.schema = [
                bigquery.SchemaField(column, "FLOAT64") for column in AMOUNTS
            ] + [bigquery.SchemaField("order_date", "DATE")]
        destination = target.reference.table(name)
        if append:
            job_id = f"olist_demo_incremental_{Path(workspace).name}"
            try:
                pending = client.load_table_from_dataframe(frame, destination, job_config=config, job_id=job_id)
            except Conflict:
                pending = client.get_job(job_id, location=location)
                logger.info("Load %s: melanjutkan job append yang sudah tercatat: %s", destination, job_id)
        else:
            pending = client.load_table_from_dataframe(frame, destination, job_config=config)
        job = pending.result()
        total_written += job.output_rows or 0
        logger.info(
            "Load %s: %s; baris siap=%s; baris hasil job=%s; job_id=%s",
            destination, disposition, len(frame), job.output_rows, job.job_id,
        )
        if dag_id == INCREMENTAL_DEMO and name == "fact_sales":
            logger.info(
                "Ringkasan fact_sales: watermark awal=%s; tanggal terakhir batch yang dimuat=%s",
                watermark, frame["order_date"].max() if len(frame) else None,
            )
    logger.info(
        "Load selesai: dataset=%s; mode=%s; tabel=%s; total baris hasil job=%s",
        dataset_id, mode, 1 + len(DIMENSION_RULES), total_written,
    )


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
        qdc = qdc_validation.override(retries=3, retry_delay=timedelta(minutes=5)) if dag_id == "demo_noise-8" else qdc_validation
        load(qdc(transform(validate_schema(extract()))))
    return demo


demo_pipeline = _build_demo("demo_pipeline", SCENARIOS["demo_pipeline"][2])
demo_noise_2 = _build_demo("demo_noise-2", SCENARIOS["demo_noise-2"][2])
demo_noise_8 = _build_demo("demo_noise-8", SCENARIOS["demo_noise-8"][2])
demo_incremental = _build_demo(INCREMENTAL_DEMO, SCENARIOS[INCREMENTAL_DEMO][2])
