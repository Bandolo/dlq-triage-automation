from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "lambda"))

import guardrails_handler as gh


def test_guardrails_allow_redrive_true():
    event = {
        "message": {
            "timestamp": "2999-01-01T00:00:00Z",
            "redriveAttempts": 0,
            "stateAtFailure": "FAILED",
        },
        "llm": {"recommended_action": "REDRIVE"},
        "max_age_days": 2,
        "max_redrive_attempts": 2,
    }
    result = gh.handler(event, None)
    assert result["guardrails"]["allow_redrive"] is True
    assert result["guardrails"]["reasons"] == []


def test_guardrails_blocks_stale():
    event = {
        "message": {
            "timestamp": "2000-01-01T00:00:00Z",
            "redriveAttempts": 0,
            "stateAtFailure": "FAILED",
        },
        "llm": {},
        "max_age_days": 2,
        "max_redrive_attempts": 2,
    }
    result = gh.handler(event, None)
    assert result["guardrails"]["allow_redrive"] is False
    assert "stale_message" in result["guardrails"]["reasons"]


def test_guardrails_blocks_attempts():
    event = {
        "message": {
            "timestamp": "2999-01-01T00:00:00Z",
            "redriveAttempts": 5,
            "stateAtFailure": "FAILED",
        },
        "llm": {},
        "max_age_days": 2,
        "max_redrive_attempts": 2,
    }
    result = gh.handler(event, None)
    assert result["guardrails"]["allow_redrive"] is False
    assert "max_attempts_exceeded" in result["guardrails"]["reasons"]


def test_guardrails_blocks_completed():
    event = {
        "message": {
            "timestamp": "2999-01-01T00:00:00Z",
            "redriveAttempts": 0,
            "stateAtFailure": "COMPLETED",
        },
        "llm": {},
        "max_age_days": 2,
        "max_redrive_attempts": 2,
    }
    result = gh.handler(event, None)
    assert result["guardrails"]["allow_redrive"] is False
    assert "already_completed" in result["guardrails"]["reasons"]


def test_guardrails_blocks_token_budget():
    event = {
        "message": {
            "timestamp": "2999-01-01T00:00:00Z",
            "redriveAttempts": 0,
            "stateAtFailure": "FAILED",
            "payload": "x" * 10000,
        },
        "llm": {},
        "max_age_days": 2,
        "max_redrive_attempts": 2,
        "max_token_estimate": 10,
    }
    result = gh.handler(event, None)
    assert result["guardrails"]["allow_redrive"] is False
    assert "token_budget_exceeded" in result["guardrails"]["reasons"]
