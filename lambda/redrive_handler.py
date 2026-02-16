import json
import os
import time
from typing import Any, Dict


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


def handler(event, _context):
    message: Dict[str, Any] = event.get("message", {})
    llm: Dict[str, Any] = event.get("llm", {})

    # Placeholder for redrive logic (re-publish to original topic / queue)
    _log(
        "INFO",
        "Redrive requested",
        correlationId=message.get("correlationId"),
        category=llm.get("category"),
        recommended_action=llm.get("recommended_action"),
    )
    _emit_metric("Redrive", 1, action="redrive")

    return {"status": "redrive_sent"}
