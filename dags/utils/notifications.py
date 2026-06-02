"""Centralized alerting and notification utilities for Datatouille pipelines."""

import logging
import os

import requests

logger = logging.getLogger(__name__)


def send_slack_alert(pipeline_name: str, message: str, severity: str = "warning") -> None:
    """Send an alert to Slack webhook if configured.

    Reads SLACK_WEBHOOK_URL from Airflow Variable or Environment.
    """
    # Retrieve webhook URL
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        try:
            from airflow.sdk import Variable

            webhook_url = Variable.get("slack_webhook_url", default="")
        except Exception:
            pass

    log_msg = f"[{severity.upper()}] Pipeline: {pipeline_name} - Message: {message}"
    logger.info("Local alert logged: %s", log_msg)

    if not webhook_url:
        logger.debug("Slack webhook not configured. Alert not sent externally.")
        return

    # Color coding based on severity
    color = "#36a64f"  # Green / info
    if severity == "error":
        color = "#ff0000"  # Red
    elif severity == "warning":
        color = "#ffa500"  # Orange

    payload = {
        "attachments": [
            {
                "color": color,
                "title": f"Datatouille Alert - {pipeline_name}",
                "text": message,
                "fields": [
                    {"title": "Pipeline", "value": pipeline_name, "short": True},
                    {"title": "Severity", "value": severity.upper(), "short": True},
                ],
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code != 200:
            logger.error(
                "Failed to send Slack notification: %d, response: %s",
                response.status_code,
                response.text,
            )
    except Exception as e:
        logger.error("Exception occurred sending Slack alert: %s", e)
