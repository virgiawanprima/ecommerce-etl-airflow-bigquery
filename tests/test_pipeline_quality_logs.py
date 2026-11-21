import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from google.cloud import bigquery

from dags import olist_etl_baseline_noqdc as baseline
from dags import olist_etl_pipeline as main_pipeline


@pytest.mark.parametrize("rejected", [0, 2, 5, 8])
def test_main_pipeline_logs_rejections_before_deciding_fail_fast(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    rejected: int,
) -> None:
    frames = {
        "fact_sales": pd.DataFrame(
            {
                "order_id": [f"o{index}" for index in range(100)],
                "order_item_id": [1] * 100,
                "product_id": ["p1"] * 100,
                "seller_id": ["s1"] * 100,
                "date_key": [20180101] * 100,
                "order_date": ["2018-01-01"] * 100,
                "price": [1.0] * (100 - rejected) + [-1.0] * rejected,
                "freight_value": [1.0] * 100,
            }
        ),
        "dim_customers": pd.DataFrame(
            {"customer_id": ["c1"], "customer_city": ["city"], "customer_state": ["SP"]}
        ),
        "dim_products": pd.DataFrame({"product_id": ["p1"]}),
        "dim_sellers": pd.DataFrame(
            {"seller_id": ["s1"], "seller_city": ["city"], "seller_state": ["SP"]}
        ),
        "dim_orders": pd.DataFrame({"order_id": ["o0"], "order_status": ["delivered"]}),
        "dim_date": pd.DataFrame({"date_key": [20180101], "date": ["2018-01-01"]}),
    }
    paths: dict[str, str] = {}
    for table_name, frame in frames.items():
        path = tmp_path / f"{table_name}.parquet"
        frame.to_parquet(path, index=False)
        paths[table_name] = str(path)
    paths["_is_incremental"] = "1"

    monkeypatch.setattr(main_pipeline, "CLEAN_DATA_DIR", str(tmp_path / "clean"))
    monkeypatch.setattr(main_pipeline, "REJECTED_DATA_DIR", str(tmp_path / "rejected"))
    caplog.set_level(logging.INFO, logger="src.utils.qdc")
    qdc = main_pipeline.dag.get_task("qdc_validation").python_callable

    if rejected > 5:
        with pytest.raises(ValueError, match="QDC fail-fast"):
            qdc(paths)
    else:
        output = qdc(paths)
        assert output["_is_incremental"] == "1"

    level = logging.WARNING if rejected else logging.INFO
    prefix = "Peringatan" if rejected else "QDC"
    message = (
        f"{prefix}: {rejected} baris ditolak pada tabel fact_sales "
        f"(total 100 baris, {rejected},00%)."
    )
    assert ("src.utils.qdc", level, message) in caplog.record_tuples
    assert len(pd.read_parquet(tmp_path / "clean" / "fact_sales_clean.parquet")) == 100 - rejected
    error_log = tmp_path / "rejected" / "fact_sales_error_log.csv"
    if rejected:
        assert len(pd.read_csv(error_log)) == rejected
    else:
        assert not error_log.exists()


def test_baseline_warns_without_claiming_a_rejection_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = Mock()
    client.query.return_value.result.return_value = [SimpleNamespace(n=0)]
    monkeypatch.setattr(bigquery, "Client", Mock(return_value=client))
    monkeypatch.setattr(baseline, "RAW_DATA_DIR", str(tmp_path / "missing"))
    monkeypatch.setattr(baseline, "STAGING_DATA_DIR", str(tmp_path / "staging"))
    caplog.set_level(logging.WARNING, logger=baseline.__name__)

    extract = baseline.dag.get_task("extract").python_callable
    with pytest.raises(FileNotFoundError):
        extract()
    baseline.dag.get_task("load").python_callable({})

    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.name == baseline.__name__ and record.levelno == logging.WARNING
    ]
    assert warnings == [baseline.BASELINE_QDC_WARNING, baseline.BASELINE_QDC_WARNING]
    assert "tidak diketahui" in baseline.BASELINE_QDC_WARNING
    client.load_table_from_dataframe.assert_not_called()
