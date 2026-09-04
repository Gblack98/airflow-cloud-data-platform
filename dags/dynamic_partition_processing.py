"""Dynamic task mapping: one mapped task instance per data partition.

The partition list is computed at runtime, so a new partition needs no DAG
redeploy. Parallelism is capped so a burst of partitions cannot saturate
(and overbill) the cluster.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# Cost control: at most 8 concurrent partition workers, whatever the backlog.
MAX_PARALLEL_PARTITIONS = 8


@dag(
    dag_id="dynamic_partition_processing",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["team:data-platform", "domain:events", "dynamic-mapping", "cost-control"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=3)},
)
def dynamic_partition_processing():
    @task
    def list_partitions(**context) -> list[str]:
        """Discover partitions for the run window (stub: replace with S3/Hive listing)."""
        ds = context["ds"]
        return [f"s3://lake/events/dt={ds}/region={r}" for r in ("eu", "us", "apac")]

    @task(max_active_tis_per_dag=MAX_PARALLEL_PARTITIONS)
    def process_partition(partition: str) -> dict:
        log.info("Processing %s", partition)
        return {"partition": partition, "rows": 0}

    @task
    def aggregate(results: list[dict]) -> None:
        total = sum(r["rows"] for r in results)
        log.info("Processed %d partitions, %d rows", len(results), total)

    aggregate(process_partition.expand(partition=list_partitions()))


dynamic_partition_processing()
