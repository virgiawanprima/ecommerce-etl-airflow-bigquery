import csv
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
from google.cloud import bigquery

from dags import olist_presentation_demos as demos
from src.utils.pii import hash_value


@pytest.fixture
def demo_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Mock]:
    raw = tmp_path / "raw"
    raw.mkdir()
    root = tmp_path / "processed" / "presentation_demos"
    monkeypatch.setattr(demos, "RAW", raw)
    monkeypatch.setattr(demos, "DEMO_ROOT", root)
    monkeypatch.setenv("GCP_PROJECT_ID", "ci-project")
    monkeypatch.setenv("BIGQUERY_DATASET", "olist_dw")
    monkeypatch.setenv("BIGQUERY_LOCATION", "asia-southeast2")

    customers = {"customer_id": "c1", "customer_unique_id": "private-c1", "customer_city": "city", "customer_state": "SP"}
    common = {
        **customers, "seller_id": "s1", "seller_city": "city", "seller_state": "SP",
        "product_id": "p1", "product_category_name": "livros", "product_category_name_english": "books",
        "order_id": "o1", "order_item_id": 1, "order_status": "delivered",
        "order_purchase_timestamp": "2018-01-01 12:00:00", "price": 24.90, "freight_value": 4.20,
        "review_id": "r1", "review_score": 5, "payment_type": "credit_card", "payment_value": 29.10,
        "geolocation_zip_code_prefix": 12345, "geolocation_lat": -23.5, "geolocation_lng": -46.6,
    }
    for alias, (filename, columns) in demos.SOURCES.items():
        row = {column: common[column] for column in columns}
        with (raw / filename).open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            if alias in {"orders", "order_items"}:
                writer.writerows({**row, "order_id": f"o{index:03d}"} for index in range(200))
            else:
                writer.writerow(row)

    client_factory = Mock(spec=bigquery.Client)
    client = client_factory.return_value
    client.project = "ci-project"
    client.create_dataset.side_effect = lambda dataset, **_: dataset
    client.load_table_from_dataframe.return_value.result.return_value = Mock(output_rows=0, job_id="ci-job")
    monkeypatch.setattr(bigquery, "Client", client_factory)
    return raw, root, client_factory


def task_callables(dag_id: str) -> list[Callable[..., object]]:
    dag = getattr(demos, dag_id.replace("-", "_"))
    return [dag.get_task(name).python_callable for name in ("extract", "validate_schema", "transform", "qdc_validation", "load")]


@pytest.mark.parametrize(
    ("dag_id", "dataset_id", "rejected", "written"),
    [
        ("demo_pipeline", "olist_demo_pipeline", 0, 200),
        ("demo_noise-2", "olist_demo_noise_2", 4, 196),
        ("demo_noise-8", "olist_demo_noise_8", 16, 0),
    ],
)
def test_demo_task_chain(
    demo_environment: tuple[Path, Path, Mock], monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    dag_id: str, dataset_id: str, rejected: int, written: int,
) -> None:
    raw, root, factory = demo_environment
    original = {path: path.read_bytes() for path in raw.glob("*.csv")}
    dag = getattr(demos, dag_id.replace("-", "_"))
    context = {"dag": dag, "run_id": "manual__ci_first"}
    monkeypatch.setattr(demos, "get_current_context", lambda: context)
    extract, validate, transform, qdc, load = task_callables(dag_id)
    workspace = Path(transform(validate(extract())))
    assert workspace.parent == root / dag_id
    context["run_id"] = "manual__ci_second"
    assert demos._workspace() != workspace
    context["run_id"] = "manual__ci_first"
    caplog.set_level(logging.INFO, logger="src.utils.qdc")

    if written:
        load(qdc(str(workspace)))
    else:
        with pytest.raises(ValueError, match=r"QDC fail-fast.*load diblokir"):
            load(qdc(str(workspace)))
        factory.assert_not_called()

    level = logging.WARNING if rejected else logging.INFO
    prefix = "Peringatan" if rejected else "QDC"
    rate = f"{rejected / 200:.2%}".replace(".", ",")
    message = (
        f"{prefix}: {rejected} baris ditolak pada tabel fact_sales "
        f"(total 200 baris, {rate})."
    )
    assert ("src.utils.qdc", level, message) in caplog.record_tuples

    assert {path: path.read_bytes() for path in raw.glob("*.csv")} == original
    rejected_csv = workspace / "rejected" / "fact_sales_error_log.csv"
    if rejected:
        errors = pd.read_csv(rejected_csv)
        assert len(errors) == rejected
        for dimension in ("completeness", "uniqueness", "consistency", "validity"):
            assert errors[f"failed_{dimension}"].any()
    else:
        assert not rejected_csv.exists()

    if written:
        client = factory.return_value
        requested = client.create_dataset.call_args.args[0]
        assert requested.dataset_id == dataset_id
        assert requested.labels == {"purpose": "olist_presentation"}
        destinations = {call.args[1].table_id: call for call in client.load_table_from_dataframe.call_args_list}
        assert set(destinations) == {"fact_sales", *demos.DIMENSION_RULES}
        for call in destinations.values():
            assert call.args[1].dataset_id == dataset_id
            assert call.kwargs["job_config"].write_disposition == "WRITE_TRUNCATE"
        fact, _ = destinations["fact_sales"].args
        assert len(fact) == written
        assert fact[["price", "freight_value"]].dtypes.astype(str).tolist() == ["float64", "float64"]
        schema = destinations["fact_sales"].kwargs["job_config"].schema
        assert {field.name: field.field_type for field in schema} == {
            "price": "FLOAT64", "freight_value": "FLOAT64", "order_date": "DATE",
        }
        assert destinations["dim_customers"].args[0]["customer_unique_id"].tolist() == [hash_value("private-c1")]


@pytest.mark.parametrize("protected", ["olist_dw", "olist_demo_pipeline"])
def test_load_refuses_protected_dataset(
    demo_environment: tuple[Path, Path, Mock], monkeypatch: pytest.MonkeyPatch, protected: str,
) -> None:
    _, _, client = demo_environment
    monkeypatch.setenv("BIGQUERY_DATASET", protected)
    monkeypatch.setitem(demos.SCENARIOS, "demo_pipeline", (0, protected, "unsafe"))
    monkeypatch.setattr(demos, "get_current_context", lambda: {"dag": demos.demo_pipeline})
    with pytest.raises(RuntimeError, match="bukan tujuan demo"):
        task_callables("demo_pipeline")[-1]("unused")
    client.assert_not_called()


def test_load_refuses_existing_dataset_without_demo_label(
    demo_environment: tuple[Path, Path, Mock], monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, factory = demo_environment
    client = factory.return_value
    existing = bigquery.Dataset("ci-project.olist_demo_pipeline")
    existing.location = "asia-southeast2"
    client.create_dataset.side_effect = None
    client.create_dataset.return_value = existing
    monkeypatch.setattr(demos, "get_current_context", lambda: {"dag": demos.demo_pipeline})
    with pytest.raises(RuntimeError, match="tidak ditandai khusus presentasi"):
        task_callables("demo_pipeline")[-1]("unused")
    client.load_table_from_dataframe.assert_not_called()


def test_invalid_source_schema_is_rejected_before_transform(
    demo_environment: tuple[Path, Path, Mock], monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, _, factory = demo_environment
    filename, _ = demos.SOURCES["order_items"]
    path = raw / filename
    frame = pd.read_csv(path).drop(columns="freight_value")
    frame.to_csv(path, index=False)
    monkeypatch.setattr(
        demos, "get_current_context", lambda: {"dag": demos.demo_pipeline, "run_id": "manual__ci_schema"},
    )
    extract, validate, _, _, _ = task_callables("demo_pipeline")
    workspace = extract()
    with pytest.raises(ValueError, match="order_items.*freight_value"):
        validate(workspace)
    assert not (Path(str(workspace)) / "transformed").exists()
    factory.assert_not_called()


@pytest.mark.parametrize(
    ("dag_id", "run_state", "qdc_state", "load_state"),
    [
        ("demo_pipeline", "success", "success", "success"),
        ("demo_noise-2", "success", "success", "success"),
        ("demo_noise-8", "failed", "failed", "upstream_failed"),
    ],
)
def test_airflow_run_states(
    demo_environment: tuple[Path, Path, Mock],
    dag_id: str, run_state: str, qdc_state: str, load_state: str,
) -> None:
    _, root, _ = demo_environment
    dag = getattr(demos, dag_id.replace("-", "_"))
    run = dag.test(logical_date=datetime(2026, 11, 19, tzinfo=timezone.utc))
    assert run.state == run_state
    assert {ti.task_id: ti.state for ti in run.get_task_instances()} == {
        "extract": "success",
        "validate_schema": "success",
        "transform": "success",
        "qdc_validation": qdc_state,
        "load": load_state,
    }
    assert (root / dag_id).is_dir()
