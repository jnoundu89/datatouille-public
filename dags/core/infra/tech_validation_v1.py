"""
Tech Validation DAG v1

This DAG validates the entire data stack infrastructure:
- MinIO S3-compatible storage connectivity and bucket operations
- Raw data upload capability
- Landing zone transformation and validation

Designed to be run manually for infrastructure validation.
All operations are idempotent and can be safely re-executed.
"""

import logging
from datetime import UTC, datetime
from typing import Any

from airflow.sdk import dag, task
from botocore.exceptions import ClientError

from utils.s3 import download_json, ensure_bucket, get_s3_client, upload_json

logger = logging.getLogger(__name__)

VALIDATION_BUCKET = "tech-validation"


@dag(
    dag_id="tech_validation_v1",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["infra", "validation"],
    description="Validates MinIO connectivity and data pipeline infrastructure",
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 2,
    },
)
def tech_validation_v1():
    """Tech validation DAG definition."""

    @task
    def create_test_bucket() -> dict[str, str]:
        """
        Create the tech-validation bucket in MinIO.

        Idempotent: If bucket exists, logs and continues successfully.

        Returns:
            dict: Status information about bucket creation

        Raises:
            Exception: On unrecoverable S3 errors
        """
        return ensure_bucket(VALIDATION_BUCKET)

    @task
    def upload_raw_json(**context) -> dict[str, Any]:
        """
        Upload test JSON data to the raw zone in MinIO.

        Idempotent: Uses logical_date for deterministic content.

        Returns:
            dict: Upload status and metadata

        Raises:
            ClientError: On upload failures
        """
        s3_key = "raw/test_data.json"
        # Use logical_date for idempotence (same result on re-run)
        logical_date = context.get("logical_date") or datetime.now(UTC)
        validation_timestamp = logical_date.isoformat()

        test_data = {
            "validation_timestamp": validation_timestamp,
            "status": "raw",
        }

        try:
            logger.info(
                "Uploading raw test data",
                extra={
                    "bucket": VALIDATION_BUCKET,
                    "key": s3_key,
                    "timestamp": validation_timestamp,
                },
            )

            upload_json(VALIDATION_BUCKET, s3_key, test_data)

            logger.info(
                "Raw test data uploaded successfully",
                extra={"bucket": VALIDATION_BUCKET, "key": s3_key},
            )

            return {
                "bucket": VALIDATION_BUCKET,
                "key": s3_key,
                "validation_timestamp": validation_timestamp,
                "status": "uploaded",
            }

        except ClientError as e:
            logger.error(
                "S3 client error during raw upload",
                extra={
                    "error_code": e.response.get("Error", {}).get("Code"),
                    "error_message": e.response.get("Error", {}).get("Message"),
                    "bucket": VALIDATION_BUCKET,
                    "key": s3_key,
                },
                exc_info=True,
            )
            raise
        except (ValueError, TypeError) as e:
            logger.error(
                "Data processing error during raw upload",
                extra={
                    "error_type": type(e).__name__,
                    "bucket": VALIDATION_BUCKET,
                    "key": s3_key,
                },
                exc_info=True,
            )
            raise

    @task
    def validate_landing_presence(raw_upload_result: dict[str, Any], **context) -> dict[str, Any]:
        """
        Transform raw data and upload to landing zone, then verify presence.

        Idempotent: Uses logical_date for deterministic timestamps.

        Args:
            raw_upload_result: Result from upload_raw_json task

        Returns:
            dict: Validation results with presence confirmation

        Raises:
            ClientError: On transformation or validation failures
        """
        raw_key = raw_upload_result["key"]
        landing_key = "landing/test_data.json"
        raw_timestamp = raw_upload_result["validation_timestamp"]
        # Use logical_date for idempotence
        logical_date = context.get("logical_date") or datetime.now(UTC)
        validated_at = logical_date.isoformat()

        try:
            # Download raw data to transform
            logger.info(
                "Downloading raw data for transformation",
                extra={"bucket": VALIDATION_BUCKET, "key": raw_key},
            )

            raw_data = download_json(VALIDATION_BUCKET, raw_key)

            # Transform to landing format
            landing_data = {
                "validation_timestamp": raw_data["validation_timestamp"],
                "status": "landing",
                "validated_at": validated_at,
            }

            logger.info(
                "Uploading transformed data to landing zone",
                extra={
                    "bucket": VALIDATION_BUCKET,
                    "key": landing_key,
                    "validated_at": validated_at,
                },
            )

            upload_json(VALIDATION_BUCKET, landing_key, landing_data)

            # Verify presence and retrieve metadata
            logger.info(
                "Verifying landing file presence",
                extra={"bucket": VALIDATION_BUCKET, "key": landing_key},
            )

            s3_client = get_s3_client()
            head_response = s3_client.head_object(
                Bucket=VALIDATION_BUCKET,
                Key=landing_key,
            )

            file_size = head_response["ContentLength"]
            last_modified = head_response["LastModified"].isoformat()

            logger.info(
                "Landing validation completed successfully",
                extra={
                    "bucket": VALIDATION_BUCKET,
                    "key": landing_key,
                    "file_size_bytes": file_size,
                    "last_modified": last_modified,
                },
            )

            return {
                "bucket": VALIDATION_BUCKET,
                "raw_key": raw_key,
                "landing_key": landing_key,
                "validation_timestamp": raw_timestamp,
                "validated_at": validated_at,
                "file_size_bytes": file_size,
                "last_modified": last_modified,
                "status": "success",
                "message": "All validation checks passed",
            }

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            logger.error(
                "S3 client error during landing validation",
                extra={
                    "error_code": error_code,
                    "error_message": e.response.get("Error", {}).get("Message"),
                    "bucket": VALIDATION_BUCKET,
                    "landing_key": landing_key,
                },
                exc_info=True,
            )
            raise
        except (ValueError, KeyError) as e:
            logger.error(
                "Data processing error during landing validation",
                extra={
                    "error_type": type(e).__name__,
                    "bucket": VALIDATION_BUCKET,
                },
                exc_info=True,
            )
            raise

    # Define task dependencies
    bucket_result = create_test_bucket()
    raw_result = upload_raw_json()
    validation_result = validate_landing_presence(raw_result)

    # Set explicit dependencies
    bucket_result >> raw_result >> validation_result


# Instantiate the DAG
tech_validation_dag = tech_validation_v1()
