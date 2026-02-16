import json
import os
import time
from typing import Any, Dict

import boto3
METRIC_NAMESPACE = os.getenv("METRIC_NAMESPACE", "DlqTriage")


def _log(level: str, message: str, **fields: Any) -> None:
    entry = {"level": level, "message": message, **fields}
    print(json.dumps(entry))


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


def _normalize(message: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "correlationId": message.get("correlationId") or message.get("id") or "unknown",
        "failureCategory": message.get("failureCategory") or message.get("category") or "UNKNOWN",
        "errorMessage": message.get("errorMessage") or message.get("error") or "",
        "timestamp": message.get("timestamp") or message.get("time") or "",
        "stateAtFailure": message.get("stateAtFailure") or message.get("state") or "FAILED",
        "redriveAttempts": int(message.get("redriveAttempts", 0)),
        "raw": message,
    }


def handler(event, _context):
    if not isinstance(event, dict):
        _log("ERROR", "Invalid event type", event_type=str(type(event)))
        _emit_metric("TriageError", 1, action="invalid_event")
        return {"status": "error"}

    state_machine_arn = os.environ["STATE_MACHINE_ARN"]
    sfn = boto3.client("stepfunctions")

    for record in event.get("Records", []):
        try:
            body = record.get("body", "{}")
            payload = json.loads(body)
            normalized = _normalize(payload)
            execution_name = f"dlq-{normalized['correlationId']}-{int(time.time())}"
            sfn.start_execution(
                stateMachineArn=state_machine_arn,
                name=execution_name,
                input=json.dumps({"message": normalized}),
            )
            _log(
                "INFO",
                "Started triage execution",
                correlationId=normalized["correlationId"],
                executionName=execution_name,
            )
            _emit_metric("TriageStarted", 1, action="start")
        except json.JSONDecodeError as exc:
            _log("ERROR", "Invalid JSON in SQS message", error=str(exc))
            _emit_metric("TriageError", 1, action="invalid_json")
            continue
        except Exception as exc:
            _log("ERROR", "Failed to process message", error=str(exc))
            _emit_metric("TriageError", 1, action="process_error")
            continue

    return {"status": "ok"}
