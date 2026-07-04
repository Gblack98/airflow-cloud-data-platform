from pathlib import Path

from airflow.models import DagBag
from airflow.models.mappedoperator import MappedOperator

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


def get_dag(dag_id):
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False).dags[dag_id]


def test_process_partition_is_mapped():
    dag = get_dag("dynamic_partition_processing")
    task = dag.get_task("process_partition")
    assert isinstance(task, MappedOperator)


def test_parallelism_is_capped():
    dag = get_dag("dynamic_partition_processing")
    assert dag.get_task("process_partition").max_active_tis_per_dag == 8


def test_map_reduce_wiring():
    dag = get_dag("dynamic_partition_processing")
    assert dag.get_task("process_partition").upstream_task_ids == {"list_partitions"}
    assert dag.get_task("aggregate").upstream_task_ids == {"process_partition"}
