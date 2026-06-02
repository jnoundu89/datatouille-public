"""MinIO/S3 client utilities.

Replaces the duplicated get_s3_client() + MINIO_* globals + create_bucket
pattern found in 10+ DAGs.
"""

import json
import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

logger = logging.getLogger(__name__)


def get_s3_client() -> Any:
    """Create MinIO S3 client from environment variables.

    Reads MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD from env.

    Raises:
        ValueError: If credentials are not configured.
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    if not access_key or not secret_key:
        raise ValueError("MinIO credentials not configured")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def ensure_bucket(bucket_name: str) -> dict[str, str]:
    """Create MinIO bucket if it doesn't exist (idempotent).

    Returns:
        Dict with 'bucket' and 'status' ('exists' or 'created').
    """
    try:
        s3 = get_s3_client()
        s3.head_bucket(Bucket=bucket_name)
        logger.info("Bucket %s already exists", bucket_name)
        return {"bucket": bucket_name, "status": "exists"}
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            s3.create_bucket(Bucket=bucket_name)
            logger.info("Created bucket %s", bucket_name)
            return {"bucket": bucket_name, "status": "created"}
        logger.error("S3 error checking bucket %s: %s", bucket_name, e)
        raise
    except (EndpointConnectionError, ConnectionError) as e:
        logger.error("Cannot connect to MinIO: %s", e)
        raise


def upload_json(bucket: str, key: str, data: Any) -> None:
    """Upload JSON-serializable data to S3."""
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Uploaded JSON to s3://%s/%s", bucket, key)


def download_json(bucket: str, key: str) -> Any:
    """Download and parse JSON from S3.

    Raises:
        ClientError: If the key doesn't exist or S3 is unreachable.
    """
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))
