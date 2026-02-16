from pathlib import Path
import json
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "lambda"))

import bedrock_adapter as ba


class DummyBody:
    def __init__(self, payload: dict):
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class DummyBedrock:
    def __init__(self, payload: dict):
        self.payload = payload

    def invoke_model(self, **_kwargs):
        return {"Body": DummyBody(self.payload)}


def test_bedrock_adapter_parses_valid_json(monkeypatch):
    payload = {
        "content": [
            {
                "text": json.dumps(
                    {
                        "category": "SYSTEM_TRANSIENT",
                        "recommended_action": "REDRIVE",
                        "confidence": 0.9,
                        "summary": "ok",
                        "reasoning": "ok",
                    }
                )
            }
        ]
    }

    monkeypatch.setattr(ba.boto3, "client", lambda _svc: DummyBedrock(payload))
    event = {"message": {"id": "1"}}
    result = ba.handler(event, None)

    assert result["llm"]["recommended_action"] == "REDRIVE"
    assert result["llm"]["category"] == "SYSTEM_TRANSIENT"


def test_bedrock_adapter_fallback_on_invalid(monkeypatch):
    payload = {"content": [{"text": "not json"}]}
    monkeypatch.setattr(ba.boto3, "client", lambda _svc: DummyBedrock(payload))
    event = {"message": {"id": "1"}}
    result = ba.handler(event, None)

    assert result["llm"]["recommended_action"] == "TICKET"
    assert result["llm"]["category"] == "UNKNOWN"
