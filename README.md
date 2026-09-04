# airflow-cloud-data-platform

Production Airflow patterns for cloud-native data platforms, each one tested:

| Pattern | Where | Point |
|---|---|---|
| Dynamic task mapping | `dags/dynamic_partition_processing.py` | runtime fan-out per partition, parallelism capped (`max_active_tis_per_dag`) |
| Kubernetes + spot | `dags/kubernetes_batch_scoring.py` | `KubernetesPodOperator` on spot nodes, requests == limits, retries absorb evictions |
| Stateful dbt retries | `dags/dbt_stateful_retry.py` | artifacts kept in GCS per run, a retry selects `result:error+` instead of rebuilding the whole project |
| Custom XCom backend | `plugins/s3_xcom_backend.py` | payloads > 4 KB offloaded to S3, metadata DB stays lean |
| Cluster policies | `plugins/cluster_policy.py` | mandatory `team:`/`domain:` tags (cost attribution), catchup banned, timeouts capped |

## Tests

```bash
pip install -r requirements.txt
pytest
```
