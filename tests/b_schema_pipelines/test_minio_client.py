"""
Tests for MinioClient.

boto3 and config loading are mocked — no real MinIO/S3 connection is made.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from b_schema_pipelines.pipelines.minio_client import MinioClient

FAKE_CONFIG = {
    "minio": {
        "endpoint": "http://localhost:9000",
        "access_key": "minio_access_key",
        "secret_key": "minio_secret_key",
    }
}


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "boom"}}, "CreateBucket")


@pytest.fixture
def client():
    """MinioClient with load_config and boto3.client mocked."""
    with patch(
        "b_schema_pipelines.pipelines.minio_client.load_config",
        return_value=FAKE_CONFIG,
    ), patch(
        "b_schema_pipelines.pipelines.minio_client.boto3.client"
    ) as mock_boto:
        mock_boto.return_value = MagicMock()
        c = MinioClient()
    return c


# ── 1. __init__ — S3 client wiring ───────────────────────────────────────────

def test_init_builds_s3_client_with_correct_minio_credentials():
    """Wrong endpoint/credentials here breaks every pipeline's MinIO access."""
    with patch(
        "b_schema_pipelines.pipelines.minio_client.load_config",
        return_value=FAKE_CONFIG,
    ), patch(
        "b_schema_pipelines.pipelines.minio_client.boto3.client"
    ) as mock_boto:
        MinioClient()

    mock_boto.assert_called_once_with(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minio_access_key",
        aws_secret_access_key="minio_secret_key",
    )


# ── 2. create_bucket — happy path ────────────────────────────────────────────

def test_create_bucket_calls_s3_with_correct_bucket_name(client):
    client.create_bucket("bronze-data")
    client.s3_client.create_bucket.assert_called_once_with(Bucket="bronze-data")


# ── 3. create_bucket — idempotent reruns ─────────────────────────────────────

def test_create_bucket_swallows_bucket_already_owned_by_you(client):
    """Airflow DAGs call this on every run — a rerun must not crash the DAG."""
    client.s3_client.create_bucket.side_effect = _client_error(
        "BucketAlreadyOwnedByYou"
    )
    client.create_bucket("bronze-data")  # must not raise


# ── 4. create_bucket — real failures must not be swallowed ──────────────────

def test_create_bucket_reraises_other_client_errors(client):
    """Only BucketAlreadyOwnedByYou is benign; anything else is a real infra
    failure (e.g. AccessDenied) and must propagate, not be hidden."""
    client.s3_client.create_bucket.side_effect = _client_error("AccessDenied")
    with pytest.raises(ClientError):
        client.create_bucket("bronze-data")


# ── 5. __init__ — fails fast on bad config ───────────────────────────────────

def test_init_propagates_config_load_errors():
    """A missing/broken config file must surface immediately, not be masked
    by a confusing KeyError deeper in __init__."""
    with patch(
        "b_schema_pipelines.pipelines.minio_client.load_config",
        side_effect=FileNotFoundError("pipeline_config.yaml"),
    ):
        with pytest.raises(FileNotFoundError):
            MinioClient()
