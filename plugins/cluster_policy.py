"""Cluster policies: platform-wide guardrails applied to every DAG and task.

Wire into airflow_local_settings.py:
    from cluster_policy import dag_policy, task_policy
"""

from __future__ import annotations

from datetime import timedelta

from airflow.exceptions import AirflowClusterPolicyViolation

# Tasks without a timeout can hang forever and hold a worker slot (= money).
MAX_EXECUTION_TIMEOUT = timedelta(hours=6)

REQUIRED_TAG_PREFIXES = ("team:", "domain:")


def dag_policy(dag) -> None:
    """Every DAG must declare ownership and stay within retention limits."""
    tags = set(dag.tags or [])
    for prefix in REQUIRED_TAG_PREFIXES:
        if not any(tag.startswith(prefix) for tag in tags):
            raise AirflowClusterPolicyViolation(
                f"DAG '{dag.dag_id}' is missing a '{prefix}*' tag "
                f"(required for cost attribution and paging)"
            )
    if dag.catchup:
        raise AirflowClusterPolicyViolation(
            f"DAG '{dag.dag_id}' has catchup=True; backfills must be explicit "
            f"(a redeploy with an old start_date would trigger a surprise backfill bill)"
        )


def task_policy(task) -> None:
    """Every task gets a bounded execution timeout."""
    if task.execution_timeout is None or task.execution_timeout > MAX_EXECUTION_TIMEOUT:
        task.execution_timeout = MAX_EXECUTION_TIMEOUT
