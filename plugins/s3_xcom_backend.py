"""Custom XCom backend: large payloads go to S3, small ones stay in the metadata DB.

The metadata database is the scheduler's hot path — stuffing DataFrame-sized
payloads into it degrades every DAG. Values above the threshold are written
to S3 and replaced by a reference string; deserialization is transparent.

Enable with:
    AIRFLOW__CORE__XCOM_BACKEND=s3_xcom_backend.S3XComBackend
"""

from __future__ import annotations

import json
import os
import uuid

from airflow.models.xcom import BaseXCom

S3_PREFIX = "s3xcom://"
MAX_INLINE_BYTES = int(os.environ.get("XCOM_MAX_INLINE_BYTES", 4096))
BUCKET = os.environ.get("XCOM_S3_BUCKET", "airflow-xcom")


def _s3_client():
    import boto3  # local import: only needed when the backend is active

    return boto3.client("s3")


class S3XComBackend(BaseXCom):
    @staticmethod
    def serialize_value(value, *, key=None, task_id=None, dag_id=None, run_id=None, map_index=None, **kwargs):
        payload = json.dumps(value)
        if len(payload.encode("utf-8")) > MAX_INLINE_BYTES:
            s3_key = f"xcom/{dag_id}/{run_id}/{task_id}/{key}-{uuid.uuid4().hex}.json"
            _s3_client().put_object(Bucket=BUCKET, Key=s3_key, Body=payload)
            value = f"{S3_PREFIX}{BUCKET}/{s3_key}"
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result):
        value = BaseXCom.deserialize_value(result)
        if isinstance(value, str) and value.startswith(S3_PREFIX):
            bucket, _, s3_key = value[len(S3_PREFIX):].partition("/")
            body = _s3_client().get_object(Bucket=bucket, Key=s3_key)["Body"].read()
            return json.loads(body)
        return value
