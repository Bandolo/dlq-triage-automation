from pathlib import Path
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import dlq_triage_sample as sample


def test_process_message_invalid_payload_missing_correlation_id(capsys):
    sample.process_message({"redriveAttempts": 0})
    captured = capsys.readouterr()
    assert "[TICKET] Create incident: UNKNOWN" in captured.out
    assert "Invalid DLQ payload." in captured.out


def test_process_message_invalid_payload_negative_attempts(capsys):
    sample.process_message({"correlationId": "c-1", "redriveAttempts": -1})
    captured = capsys.readouterr()
    assert "[TICKET] Create incident: c-1" in captured.out
    assert "Invalid DLQ payload." in captured.out


def test_guardrails_malformed_timestamp_forces_ticket():
    message = sample.DLQMessage(
        correlationId="c-2",
        failureCategory="DOWNSTREAM_TIMEOUT",
        errorMessage="Timeout",
        timestamp="not-a-timestamp",
        stateAtFailure="FAILED",
        redriveAttempts=0,
    )
    decision = sample.Decision(
        category="SYSTEM_TRANSIENT",
        recommended_action="REDRIVE",
        confidence=0.9,
        summary="Transient timeout.",
        reasoning="Replayable.",
    )
    assert sample.guardrails(message, decision) == "TICKET"


def test_decision_confidence_bounds():
    with pytest.raises(ValueError):
        sample.Decision(
            category="SYSTEM_TRANSIENT",
            recommended_action="REDRIVE",
            confidence=1.5,
            summary="Too high.",
            reasoning="Invalid bounds.",
        )
