import json
from unittest.mock import MagicMock, patch

from s3_xcom_backend import MAX_INLINE_BYTES, S3_PREFIX, S3XComBackend


def test_small_value_stays_inline():
    with patch("s3_xcom_backend._s3_client") as client:
        stored = S3XComBackend.serialize_value(
            {"rows": 12}, key="k", task_id="t", dag_id="d", run_id="r"
        )
    client.assert_not_called()
    assert S3XComBackend.deserialize_value(
        MagicMock(value=stored)
    ) == {"rows": 12}


def test_large_value_offloaded_to_s3():
    big = {"payload": "x" * (MAX_INLINE_BYTES + 1)}
    s3 = MagicMock()
    with patch("s3_xcom_backend._s3_client", return_value=s3):
        stored = S3XComBackend.serialize_value(
            big, key="k", task_id="t", dag_id="d", run_id="r"
        )

    s3.put_object.assert_called_once()
    put = s3.put_object.call_args.kwargs
    assert json.loads(put["Body"]) == big
    assert put["Key"].startswith("xcom/d/r/t/k-")

    # what lands in the metadata DB is only the reference
    reference = json.loads(stored)
    assert reference.startswith(S3_PREFIX)
    assert len(stored) < 200


def test_large_value_roundtrip():
    big = {"payload": "x" * (MAX_INLINE_BYTES + 1)}
    s3 = MagicMock()
    with patch("s3_xcom_backend._s3_client", return_value=s3):
        stored = S3XComBackend.serialize_value(
            big, key="k", task_id="t", dag_id="d", run_id="r"
        )
        s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: json.dumps(big).encode())
        }
        value = S3XComBackend.deserialize_value(MagicMock(value=stored))

    assert value == big
    bucket = s3.get_object.call_args.kwargs["Bucket"]
    assert bucket == "airflow-xcom"
