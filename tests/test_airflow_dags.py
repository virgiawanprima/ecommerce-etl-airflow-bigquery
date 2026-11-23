from pathlib import Path
from datetime import timedelta

from airflow.models import DagBag
from airflow.timetables.simple import NullTimetable


def test_repository_dags_import_and_have_expected_dependencies() -> None:
    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    full = ("extract", "validate_schema", "transform", "qdc_validation", "load")
    baseline = ("extract", "validate_schema", "transform", "load")
    expected = {
        "olist_etl_pipeline": full,
        "olist_etl_baseline_noqdc": baseline,
        "demo_pipeline": full,
        "demo_noise-2": full,
        "demo_noise-8": full,
        "demo_incremental": full,
    }
    assert expected.keys() <= bag.dags.keys()
    for dag_id, chain in expected.items():
        dag = bag.dags[dag_id]
        assert set(dag.task_ids) == set(chain), dag_id
        for index, task_id in enumerate(chain):
            task = dag.get_task(task_id)
            assert task.upstream_task_ids == set(chain[max(0, index - 1):index]), (dag_id, task_id)
            assert task.trigger_rule == "all_success", (dag_id, task_id)
        if dag_id.startswith("demo"):
            assert isinstance(dag.timetable, NullTimetable)
            assert dag.max_active_runs == 1
            for task in dag.tasks:
                if dag_id == "demo_noise-8" and task.task_id == "qdc_validation":
                    assert task.retries == 3
                    assert task.retry_delay == timedelta(minutes=5)
                else:
                    assert task.retries == 0
