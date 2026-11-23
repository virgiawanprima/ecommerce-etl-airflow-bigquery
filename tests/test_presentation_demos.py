import csv
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
from google.api_core.exceptions import Conflict, Forbidden, NotFound
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
    caplog.set_level(logging.INFO, logger="src.utils.qdc")
    caplog.set_level(logging.INFO, logger=demos.__name__)
    extract, validate, transform, qdc, load = task_callables(dag_id)
    workspace = Path(transform(validate(extract())))
    assert workspace.parent == root / dag_id
    context["run_id"] = "manual__ci_second"
    assert demos._workspace() != workspace
    context["run_id"] = "manual__ci_first"
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
    assert f"run_id=manual__ci_first; dataset={dataset_id}" in caplog.text
    assert "sumber=olist_order_items_dataset.csv; baris=200" in caplog.text
    if written:
        assert "load diizinkan" in caplog.text
        assert f"Load selesai: dataset={dataset_id}; mode=FULL; tabel=6" in caplog.text
        assert f"baris siap={written};" in caplog.text
        assert "job_id=ci-job" in caplog.text
    if rejected:
        assert "alasan={'completeness':" in caplog.text
    assert "private-c1" not in caplog.text

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


def test_incremental_demo_seeds_appends_and_skips_when_current(
    demo_environment: tuple[Path, Path, Mock], monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw, _, factory = demo_environment
    orders_path = raw / demos.SOURCES["orders"][0]
    orders = pd.read_csv(orders_path)
    orders.loc[100:, "order_purchase_timestamp"] = "2018-01-02 12:00:00"
    orders.to_csv(orders_path, index=False)

    client = factory.return_value
    query_result = client.query.return_value.result
    query_result.side_effect = [
        NotFound("fact_sales belum ada"),
        [Mock(wm=datetime(2018, 1, 1).date())],
        [Mock(wm=datetime(2018, 1, 2).date())],
        Forbidden("akses ditolak"),
    ]
    context = {"dag": demos.demo_incremental, "run_id": ""}
    monkeypatch.setattr(demos, "get_current_context", lambda: context)
    caplog.set_level(logging.INFO)
    extract, validate, transform, qdc, load = task_callables("demo_incremental")

    for index, disposition, date_string in [
        (1, "WRITE_TRUNCATE", "2018-01-01"), (2, "WRITE_APPEND", "2018-01-02"),
    ]:
        context["run_id"] = f"manual__incremental_{index}"
        client.load_table_from_dataframe.reset_mock()
        load(qdc(transform(validate(extract()))))
        calls = client.load_table_from_dataframe.call_args_list
        assert len(calls) == 6
        assert all(call.kwargs["job_config"].write_disposition == "WRITE_TRUNCATE" for call in calls[:-1])
        frame, destination = calls[-1].args
        assert destination.dataset_id == "olist_demo_incremental"
        assert destination.table_id == "fact_sales"
        assert len(frame) == 100
        assert set(frame["order_date"].astype(str)) == {date_string}
        assert calls[-1].kwargs["job_config"].write_disposition == disposition

    context["run_id"] = "manual__incremental_3"
    client.load_table_from_dataframe.reset_mock()
    load(qdc(transform(validate(extract()))))
    client.load_table_from_dataframe.assert_not_called()
    assert "mode=INCREMENTAL" in caplog.text
    assert "kandidat QDC=100/200" in caplog.text
    assert "pemuatan dilewati" in caplog.text

    context["run_id"] = "manual__incremental_4"
    with pytest.raises(Forbidden):
        extract()
    client.load_table_from_dataframe.assert_not_called()
    assert "akses ditolak; extract dihentikan, tidak beralih ke FULL" in caplog.text


def test_airflow_incremental_runs_with_persisted_facts(
    demo_environment: tuple[Path, Path, Mock], caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, root, factory = demo_environment
    orders_path = raw / demos.SOURCES["orders"][0]
    orders = pd.read_csv(orders_path)
    orders.loc[100:, "order_purchase_timestamp"] = "2018-01-02 12:00:00"
    orders.to_csv(orders_path, index=False)

    client = factory.return_value
    facts: list[pd.DataFrame] = []
    jobs: dict[str, Mock] = {}
    fact_writes: list[tuple[str, int]] = []

    def read_watermark() -> list[Mock]:
        if not facts:
            raise NotFound("fact_sales belum ada")
        return [Mock(wm=max(frame["order_date"].max() for frame in facts))]

    def write_table(
        frame: pd.DataFrame, destination: bigquery.TableReference, *,
        job_config: bigquery.LoadJobConfig, job_id: str | None = None,
    ) -> Mock:
        if job_id and job_id in jobs:
            raise Conflict("Job append sudah ada")
        if destination.table_id == "fact_sales":
            assert destination.dataset_id == "olist_demo_incremental"
            if job_config.write_disposition == "WRITE_TRUNCATE":
                facts.clear()
            facts.append(frame.copy())
            fact_writes.append((job_config.write_disposition, len(frame)))
        job = Mock()
        job.result.return_value = Mock(output_rows=len(frame), job_id=job_id or "simulated-job")
        if job_id:
            jobs[job_id] = job
        return job

    client.query.return_value.result.side_effect = read_watermark
    client.load_table_from_dataframe.side_effect = write_table
    client.get_job.side_effect = lambda job_id, **_: jobs[job_id]
    caplog.set_level(logging.INFO)

    for index, expected_writes in enumerate((6, 6, 0), start=1):
        client.load_table_from_dataframe.reset_mock()
        run = demos.demo_incremental.test(logical_date=datetime(2026, 11, index, tzinfo=timezone.utc))
        assert run.state == "success"
        assert len(run.get_task_instances()) == 5
        assert {ti.state for ti in run.get_task_instances()} == {"success"}
        assert client.load_table_from_dataframe.call_count == expected_writes
        assert sum(len(frame) for frame in facts) == (100 if index == 1 else 200)
        if index == 2:
            append_job_id = next(iter(jobs))
            workspace = root / "demo_incremental" / append_job_id.removeprefix("olist_demo_incremental_")
            with monkeypatch.context() as patch:
                patch.setattr(demos, "get_current_context", lambda: {"dag": demos.demo_incremental})
                task_callables("demo_incremental")[-1](str(workspace))
            client.get_job.assert_called_once_with(append_job_id, location="asia-southeast2")

    assert fact_writes == [("WRITE_TRUNCATE", 100), ("WRITE_APPEND", 100)]
    assert not pd.concat(facts).duplicated(subset=["order_id", "order_item_id"]).any()
    assert "melanjutkan job append yang sudah tercatat" in caplog.text
    assert "pemuatan dilewati untuk semua tabel" in caplog.text


def test_airflow_incremental_forbidden_watermark_blocks_load(
    demo_environment: tuple[Path, Path, Mock],
) -> None:
    _, _, factory = demo_environment
    client = factory.return_value
    client.query.return_value.result.side_effect = Forbidden("akses ditolak")
    run = demos.demo_incremental.test(logical_date=datetime(2026, 11, 4, tzinfo=timezone.utc))
    assert run.state == "failed"
    assert {ti.task_id: ti.state for ti in run.get_task_instances()} == {
        "extract": "failed",
        "validate_schema": "upstream_failed",
        "transform": "upstream_failed",
        "qdc_validation": "upstream_failed",
        "load": "upstream_failed",
    }
    client.create_dataset.assert_not_called()
    client.load_table_from_dataframe.assert_not_called()


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
    demo_environment: tuple[Path, Path, Mock],
) -> None:
    raw, root, factory = demo_environment
    filename, _ = demos.SOURCES["order_items"]
    path = raw / filename
    frame = pd.read_csv(path).drop(columns="freight_value")
    frame.to_csv(path, index=False)
    run = demos.demo_pipeline.test(logical_date=datetime(2026, 11, 17, tzinfo=timezone.utc))
    assert run.state == "failed"
    assert {ti.task_id: ti.state for ti in run.get_task_instances()} == {
        "extract": "success",
        "validate_schema": "failed",
        "transform": "upstream_failed",
        "qdc_validation": "upstream_failed",
        "load": "upstream_failed",
    }
    assert not list((root / "demo_pipeline").glob("*/transformed"))
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
    monkeypatch: pytest.MonkeyPatch,
    dag_id: str, run_state: str, qdc_state: str, load_state: str,
) -> None:
    _, root, _ = demo_environment
    dag = getattr(demos, dag_id.replace("-", "_"))
    monkeypatch.setattr(dag.get_task("qdc_validation"), "retries", 0)
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


@pytest.mark.parametrize(("rejected", "load_state"), [(10, "success"), (11, "upstream_failed")])
def test_airflow_qdc_boundary(
    demo_environment: tuple[Path, Path, Mock], rejected: int, load_state: str,
) -> None:
    raw, _, factory = demo_environment
    path = raw / demos.SOURCES["order_items"][0]
    frame = pd.read_csv(path)
    frame.loc[:rejected - 1, "price"] = -1
    frame.to_csv(path, index=False)

    run = demos.demo_pipeline.test(logical_date=datetime(2026, 11, 18, tzinfo=timezone.utc))
    states = {ti.task_id: ti.state for ti in run.get_task_instances()}
    assert states["load"] == load_state
    assert states["qdc_validation"] == ("success" if load_state == "success" else "failed")
    if load_state == "success":
        assert run.state == "success"
        calls = factory.return_value.load_table_from_dataframe.call_args_list
        fact = next(call.args[0] for call in calls if call.args[1].table_id == "fact_sales")
        assert len(fact) == 200 - rejected
    else:
        assert run.state == "failed"
        factory.assert_not_called()
