"""Nightly batch scoring on Kubernetes, tuned for cost.

Cost levers, in order of impact:
1. spot/preemptible nodes via nodeSelector + toleration (retries absorb evictions)
2. explicit resource requests == limits (no overprovisioned pods)
3. one-off pods (no idle workers between runs)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

SPOT_TOLERATION = k8s.V1Toleration(
    key="cloud.google.com/gke-spot", operator="Equal", value="true", effect="NoSchedule"
)

SCORING_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "2", "memory": "4Gi"},
    limits={"cpu": "2", "memory": "4Gi"},
)

with DAG(
    dag_id="kubernetes_batch_scoring",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["team:ml-platform", "domain:scoring", "kubernetes", "spot", "cost-control"],
    default_args={
        # 3 retries: spot evictions are expected, not exceptional
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:
    score = KubernetesPodOperator(
        task_id="score_customers",
        name="score-customers",
        namespace="batch-jobs",
        image="registry.example.com/ml/scoring:1.4.2",
        arguments=["python", "-m", "scoring.batch", "--date", "{{ ds }}"],
        container_resources=SCORING_RESOURCES,
        node_selector={"cloud.google.com/gke-spot": "true"},
        tolerations=[SPOT_TOLERATION],
        on_finish_action="delete_pod",
        startup_timeout_seconds=300,
        get_logs=True,
    )

    publish = KubernetesPodOperator(
        task_id="publish_scores",
        name="publish-scores",
        namespace="batch-jobs",
        image="registry.example.com/ml/scoring:1.4.2",
        arguments=["python", "-m", "scoring.publish", "--date", "{{ ds }}"],
        # publish is short and idempotence-critical: on-demand nodes, no spot
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "512Mi"},
            limits={"cpu": "500m", "memory": "512Mi"},
        ),
        on_finish_action="delete_pod",
        get_logs=True,
    )

    score >> publish
