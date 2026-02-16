import json
import os
import time
from typing import Any, Dict

import boto3
METRIC_NAMESPACE = os.getenv("METRIC_NAMESPACE", "DlqTriage")


def _emit_metric(name: str, value: float, unit: str = "Count", **dims: str) -> None:
    emf = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": METRIC_NAMESPACE,
                    "Dimensions": [list(dims.keys())] if dims else [[]],
                    "Metrics": [{"Name": name, "Unit": unit}],
                }
            ],
        },
        name: value,
        **dims,
    }
    print(json.dumps(emf))


def handler(event, _context):
    queue_url = os.environ["DLQ_QUEUE_URL"]
    sqs = boto3.client("sqs")

    # Accept a provided message or use a default example
    message: Dict[str, Any] = event.get("message") if isinstance(event, dict) else None
    if not message:
        message = {
            "correlationId": "sample-123",
            "failureCategory": "DOWNSTREAM_TIMEOUT",
            "errorMessage": "Timeout after 3 retries",
            "timestamp": "2025-01-15T10:36:00Z",
            "stateAtFailure": "FAILED",
            "redriveAttempts": 0,
        }

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message),
    )

    _emit_metric("ProducerSent", 1, action="producer")
    return {"status": "sent", "queue_url": queue_url}
