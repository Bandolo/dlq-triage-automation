from pathlib import Path
import json
import sys
import time

sys.path.append(str(Path(__file__).resolve().parents[1] / "lambda"))

import triage_handler as th


class DummySfn:
    def __init__(self):
        self.calls = []

    def start_execution(self, stateMachineArn, name, input):
        self.calls.append({"stateMachineArn": stateMachineArn, "name": name, "input": input})
        return {"executionArn": "arn:aws:states:sample"}


def test_triage_starts_execution(monkeypatch):
    dummy = DummySfn()
    monkeypatch.setattr(th.boto3, "client", lambda service: dummy)
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:sample")

    event = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "correlationId": "c-1",
                        "failureCategory": "DOWNSTREAM_TIMEOUT",
                        "errorMessage": "Timeout",
                        "timestamp": "2025-01-15T10:36:00Z",
                        "stateAtFailure": "FAILED",
                        "redriveAttempts": 1,
                    }
                )
            }
        ]
    }

    before = int(time.time())
    th.handler(event, None)

    assert dummy.calls
    call = dummy.calls[0]
    assert call["stateMachineArn"] == "arn:aws:states:sample"
    assert call["name"].startswith("dlq-c-1-")
    payload = json.loads(call["input"])
    assert payload["message"]["correlationId"] == "c-1"
    assert payload["message"]["redriveAttempts"] == 1
    assert int(call["name"].split("-")[-1]) >= before
