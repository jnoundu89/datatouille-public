"""MinIO/S3 client utilities.

Replaces the duplicated get_s3_client() + MINIO_* globals + create_bucket
pattern found in 10+ DAGs.
"""

import gzip
import json
import logging
import os
from datetime import UTC, datetime
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
    """Upload JSON-serializable data to S3, with automatic Gzip compression if key ends in .gz."""
    s3 = get_s3_client()
    indent_val = None if key.endswith(".gz") else 2
    body_bytes = json.dumps(data, ensure_ascii=False, indent=indent_val).encode("utf-8")

    extra_args = {}
    if key.endswith(".gz"):
        body_bytes = gzip.compress(body_bytes)
        extra_args["ContentEncoding"] = "gzip"

    s3.put_object(Bucket=bucket, Key=key, Body=body_bytes, ContentType="application/json", **extra_args)
    logger.info("Uploaded JSON%s to s3://%s/%s", " (gzipped)" if key.endswith(".gz") else "", bucket, key)


def download_json(bucket: str, key: str) -> Any:
    """Download and parse JSON from S3, with automatic Gzip decompression if key ends in .gz."""
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    body_bytes = obj["Body"].read()

    # Decompress if file is gzipped based on extension or header metadata
    if key.endswith(".gz") or obj.get("ContentEncoding") == "gzip":
        body_bytes = gzip.decompress(body_bytes)

    return json.loads(body_bytes.decode("utf-8"))


def get_partitioned_key(domain: str, source: str, filename: str, date_str: str) -> str:
    """Generate a Hive-style partitioned S3 key.

    Example:
        get_partitioned_key("gaming", "itad", "deals.json.gz", "2026-06-02")
        -> "domain=gaming/source=itad/year=2026/month=06/day=02/deals.json.gz"
    """
    try:
        # Support full ISO timestamp strings as well
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        dt = datetime.now(UTC)

    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    return f"domain={domain}/source={source}/year={year}/month={month}/day={day}/{filename}"


def upload_parquet(bucket: str, key: str, data: list[dict] | Any) -> None:
    """Upload list of dicts or pandas DataFrame as Parquet to S3 using an in-memory buffer."""
    import io

    import pandas as pd

    s3 = get_s3_client()
    df = pd.DataFrame(data) if isinstance(data, list) else data

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue(), ContentType="application/octet-stream")
    logger.info("Uploaded Parquet to s3://%s/%s", bucket, key)


def download_parquet(bucket: str, key: str) -> Any:
    """Download Parquet from S3 and return as a pandas DataFrame."""
    import io

    import pandas as pd

    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    body_bytes = obj["Body"].read()

    buffer = io.BytesIO(body_bytes)
    return pd.read_parquet(buffer)
