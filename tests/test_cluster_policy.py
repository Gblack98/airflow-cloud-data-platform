from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowClusterPolicyViolation
from airflow.models import DagBag

from cluster_policy import MAX_EXECUTION_TIMEOUT, dag_policy, task_policy

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


def fake_dag(tags, catchup=False, dag_id="demo"):
    return SimpleNamespace(dag_id=dag_id, tags=tags, catchup=catchup)


def test_dag_without_team_tag_rejected():
    with pytest.raises(AirflowClusterPolicyViolation, match="team:"):
        dag_policy(fake_dag(tags=["domain:sales"]))


def test_dag_with_catchup_rejected():
    with pytest.raises(AirflowClusterPolicyViolation, match="catchup"):
        dag_policy(fake_dag(tags=["team:a", "domain:b"], catchup=True))


def test_compliant_dag_passes():
    dag_policy(fake_dag(tags=["team:a", "domain:b"]))


def test_task_timeout_is_capped():
    task = SimpleNamespace(execution_timeout=None)
    task_policy(task)
    assert task.execution_timeout == MAX_EXECUTION_TIMEOUT

    task = SimpleNamespace(execution_timeout=timedelta(days=2))
    task_policy(task)
    assert task.execution_timeout == MAX_EXECUTION_TIMEOUT

    task = SimpleNamespace(execution_timeout=timedelta(minutes=30))
    task_policy(task)
    assert task.execution_timeout == timedelta(minutes=30)


def test_all_shipped_dags_comply_with_policy():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag in dagbag.dags.values():
        dag_policy(dag)
