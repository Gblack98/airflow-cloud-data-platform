"""Nightly dbt run where a retry replays only the failed models.

Each attempt gets a fresh pod, so the artifacts of the previous attempt are kept
in GCS rather than in the container. The selection happens in the image
(docker/dbt/entrypoint.sh): Airflow decides when to retry, dbt decides what.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

IMAGE = "registry.example.com/data/dbt:1.8.3"

DBT_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "1", "memory": "2Gi"},
    limits={"cpu": "1", "memory": "2Gi"},
)


def dbt_task(task_id: str, select: str) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace="data-jobs",
        image=IMAGE,
        env_vars={
            "DBT_STATE_BUCKET": "acme-dbt-state",
            "DBT_SELECT": select,
            # scoped to the run, otherwise tonight replays last night's failures
            "DAG_ID": "{{ dag.dag_id }}",
            "RUN_ID": "{{ run_id }}",
        },
        container_resources=DBT_RESOURCES,
        on_finish_action="delete_pod",
        get_logs=True,
    )


with DAG(
    dag_id="dbt_stateful_retry",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["team:analytics-eng", "domain:warehouse", "kubernetes", "dbt"],
    default_args={
        # replaying a handful of models is cheap, so we can afford to insist
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    staging = dbt_task("dbt_staging", "staging")
    warehouse = dbt_task("dbt_warehouse", "marts")

    staging >> warehouse
