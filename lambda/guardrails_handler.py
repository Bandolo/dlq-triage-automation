import json
import os
import time
from typing import Any, Dict

# Placeholder idempotency check (replace with real service)

def _is_duplicate(_message: Dict[str, Any]) -> bool:
    return False


def _is_older_than_days(timestamp: str, max_days: int) -> bool:
    try:
        date_part = timestamp.split("T", 1)[0]
        year, month, day = map(int, date_part.split("-"))
        days = (time.time() - time.mktime((year, month, day, 0, 0, 0, 0, 0, -1))) / 86400
        return days > max_days
    except Exception as exc:
        print(json.dumps({"level": "WARN", "message": "Invalid timestamp", "timestamp": timestamp, "error": str(exc)}))
        return True


def handler(event, _context):
    message: Dict[str, Any] = event.get("message", {})
    llm: Dict[str, Any] = event.get("llm", {})

    metric_namespace = os.getenv("METRIC_NAMESPACE", "DlqTriage")
    max_age_days = int(event.get("max_age_days", 2))
    max_redrive_attempts = int(event.get("max_redrive_attempts", 2))
    max_token_estimate = int(event.get("max_token_estimate", 2000))

    ts = message.get("timestamp") or ""
    attempts = int(message.get("redriveAttempts", 0))
    already_completed = (message.get("stateAtFailure") or "").upper() == "COMPLETED"
    stale = _is_older_than_days(ts, max_age_days)
    duplicate = _is_duplicate(message)
    token_estimate = max(1, len(json.dumps(message)) // 4)

    allow_redrive = True
    reasons = []

    if stale:
        allow_redrive = False
        reasons.append("stale_message")
    if attempts >= max_redrive_attempts:
        allow_redrive = False
        reasons.append("max_attempts_exceeded")
    if already_completed:
        allow_redrive = False
        reasons.append("already_completed")
    if duplicate:
        allow_redrive = False
        reasons.append("duplicate_message")
    if token_estimate > max_token_estimate:
        allow_redrive = False
        reasons.append("token_budget_exceeded")

    result = {
        "message": message,
        "llm": llm,
        "guardrails": {
            "allow_redrive": allow_redrive,
            "reasons": reasons,
            "max_age_days": max_age_days,
            "max_redrive_attempts": max_redrive_attempts,
            "token_estimate": token_estimate,
            "max_token_estimate": max_token_estimate,
        },
    }

    category = str(llm.get("category") or "UNKNOWN")
    emf_category = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": metric_namespace,
                    "Dimensions": [["category"]],
                    "Metrics": [{"Name": "CategoryCount", "Unit": "Count"}],
                }
            ],
        },
        "category": category,
        "CategoryCount": 1,
    }
    print(json.dumps(emf_category))

    emf = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": metric_namespace,
                    "Dimensions": [["action"]],
                    "Metrics": [{"Name": "GuardrailEvaluation", "Unit": "Count"}],
                }
            ],
        },
        "action": "guardrails",
        "GuardrailEvaluation": 1,
        "allow_redrive": allow_redrive,
    }
    print(json.dumps(emf))
    print(json.dumps(result))
    return result
