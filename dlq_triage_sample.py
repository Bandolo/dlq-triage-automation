"""DLQ triage sample: classify -> decision -> action.

Runs with AWS SQS if AWS creds are configured, otherwise falls back to an in-memory queue.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict

from pydantic import BaseModel, Field, ValidationError

try:  # Pydantic v2
    from pydantic import field_validator
    _USE_V2_VALIDATOR = True
except Exception:  # pragma: no cover - Pydantic v1 fallback
    from pydantic import validator as field_validator
    _USE_V2_VALIDATOR = False

try:
    import boto3  # type: ignore
    from botocore.exceptions import BotoCoreError, NoCredentialsError  # type: ignore
except Exception:  # pragma: no cover
    boto3 = None
    BotoCoreError = NoCredentialsError = Exception


class Decision(BaseModel):
    category: str
    recommended_action: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    summary: str
    reasoning: str


class DLQMessage(BaseModel):
    correlationId: str
    failureCategory: str | None = None
    errorMessage: str | None = None
    timestamp: str | None = None
    stateAtFailure: str | None = None
    redriveAttempts: int = 0

    if _USE_V2_VALIDATOR:
        @field_validator("redriveAttempts")
        @classmethod
        def non_negative_attempts(cls, value: int) -> int:
            if value < 0:
                raise ValueError("redriveAttempts must be >= 0")
            return value
    else:
        @field_validator("redriveAttempts")
        def non_negative_attempts(cls, value: int) -> int:
            if value < 0:
                raise ValueError("redriveAttempts must be >= 0")
            return value


ALLOWLIST = {"SYSTEM_TRANSIENT"}
CONFIDENCE_THRESHOLD = 0.8
MAX_REDRIVE_AGE_DAYS = 2
MAX_REDRIVE_ATTEMPTS = 2


def classify(message: DLQMessage) -> Decision:
    """Stub classifier. Replace with Bedrock or your LLM of choice."""
    error = (message.errorMessage or "").lower()
    failure_category = (message.failureCategory or "").lower()

    if "timeout" in error or "retry" in error or "transient" in failure_category:
        return Decision(
            category="SYSTEM_TRANSIENT",
            recommended_action="REDRIVE",
            confidence=0.91,
            summary="Transient timeout after retries.",
            reasoning="Timeouts after retries are typically replayable once downstream recovers.",
        )

    if "invalid" in error or "schema" in error:
        return Decision(
            category="DATA_QUALITY",
            recommended_action="TICKET",
            confidence=0.72,
            summary="Payload appears invalid.",
            reasoning="Invalid schema requires manual inspection; do not redrive automatically.",
        )

    return Decision(
        category="UNKNOWN",
        recommended_action="TICKET",
        confidence=0.5,
        summary="Unknown failure type.",
        reasoning="Unclear root cause. Escalate for analysis.",
    )


def guardrails(message: DLQMessage, decision: Decision) -> str:
    """Apply safety checks to decide final action."""
    # Confidence gate
    if decision.confidence < CONFIDENCE_THRESHOLD:
        return "TICKET"

    # Category allowlist gate
    if decision.category not in ALLOWLIST:
        return "TICKET"

    # Age gate
    ts = message.timestamp
    if ts and is_older_than_days(ts, MAX_REDRIVE_AGE_DAYS):
        return "TICKET"

    # Redrive attempts gate
    attempts = message.redriveAttempts
    if attempts >= MAX_REDRIVE_ATTEMPTS:
        return "TICKET"

    # Terminal state gate
    if (message.stateAtFailure or "").upper() == "COMPLETED":
        return "SUPPRESS"

    return decision.recommended_action


def is_older_than_days(timestamp: str, max_days: int) -> bool:
    try:
        # Minimal parsing: YYYY-MM-DDTHH:MM:SSZ
        date_part = timestamp.split("T", 1)[0]
        year, month, day = map(int, date_part.split("-"))
        days = (time.time() - time.mktime((year, month, day, 0, 0, 0, 0, 0, -1))) / 86400
        return days > max_days
    except Exception:
        return True


def action_redrive(message: DLQMessage) -> None:
    print(f"[REDRIVE] Replaying message {message.correlationId}")


def action_suppress(message: DLQMessage) -> None:
    print(f"[SUPPRESS] Marking message {message.correlationId} as non-replayable")


def action_ticket(message: DLQMessage, decision: Decision) -> None:
    print(
        f"[TICKET] Create incident: {message.correlationId} | "
        f"{decision.category} | {decision.summary}"
    )


def _validate_message(message: Dict[str, Any]) -> DLQMessage:
    if hasattr(DLQMessage, "model_validate"):
        return DLQMessage.model_validate(message)
    return DLQMessage.parse_obj(message)


def process_message(message: Dict[str, Any]) -> None:
    try:
        parsed = _validate_message(message)
    except ValidationError as exc:
        bad_message = DLQMessage(
            correlationId=message.get("correlationId", "UNKNOWN"),
            failureCategory=message.get("failureCategory"),
            errorMessage=message.get("errorMessage"),
            timestamp=message.get("timestamp"),
            stateAtFailure=message.get("stateAtFailure"),
            redriveAttempts=0,
        )
        decision = Decision(
            category="DATA_QUALITY",
            recommended_action="TICKET",
            confidence=0.0,
            summary="Invalid DLQ payload.",
            reasoning=str(exc),
        )
        action_ticket(bad_message, decision)
        return

    decision = classify(parsed)
    final_action = guardrails(parsed, decision)

    if final_action == "REDRIVE":
        action_redrive(parsed)
    elif final_action == "SUPPRESS":
        action_suppress(parsed)
    else:
        action_ticket(parsed, decision)


def in_memory_sample() -> None:
    sample = {
        "correlationId": "0194e12c-13c4-7358-bf00-d40b0d69497b",
        "failureCategory": "DOWNSTREAM_TIMEOUT",
        "errorMessage": "Timeout after 3 retries",
        "timestamp": "2025-01-15T10:36:00Z",
        "stateAtFailure": "FAILED",
        "redriveAttempts": 0,
    }
    print("[SAMPLE] Using in-memory queue")
    process_message(sample)


def sqs_sample(queue_url: str) -> None:
    if boto3 is None:
        raise RuntimeError("boto3 not available; install boto3 or run in-memory sample.")

    sqs = boto3.client("sqs")
    resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
    messages = resp.get("Messages", [])
    if not messages:
        print("[SQS] No messages available")
        return

    msg = messages[0]
    body = json.loads(msg["Body"])
    process_message(body)

    # delete after processing
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])


def main() -> None:
    queue_url = os.getenv("DLQ_QUEUE_URL")
    if queue_url:
        try:
            print("[SAMPLE] Using AWS SQS")
            sqs_sample(queue_url)
            return
        except (BotoCoreError, NoCredentialsError) as exc:
            print(f"[WARN] SQS unavailable: {exc}; falling back to in-memory sample")

    in_memory_sample()


if __name__ == "__main__":
    main()
