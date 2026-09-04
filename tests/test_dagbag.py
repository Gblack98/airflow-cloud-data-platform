from pathlib import Path

from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


def test_dagbag_imports_without_errors():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert dagbag.import_errors == {}
    assert set(dagbag.dags) == {
        "dynamic_partition_processing",
        "kubernetes_batch_scoring",
        "dbt_stateful_retry",
    }


def test_no_dag_uses_catchup():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag in dagbag.dags.values():
        assert dag.catchup is False, f"{dag.dag_id} must not backfill implicitly"
