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
    def __init__(self, payload: dict, error: Exception = None):
        self.payload = payload
        self.error = error

    def invoke_model(self, **_kwargs):
        if self.error:
            raise self.error
        return {"Body": DummyBody(self.payload)}


def test_bedrock_adapter_missing_required_fields(monkeypatch):
    payload = {"content": [{"text": json.dumps({"category": "X"})}]}
    monkeypatch.setattr(ba.boto3, "client", lambda _svc: DummyBedrock(payload))
    result = ba.handler({"message": {"id": "1"}}, None)
    assert result["llm"]["recommended_action"] == "TICKET"


def test_bedrock_adapter_invalid_confidence_type(monkeypatch):
    payload = {
        "content": [
            {
                "text": json.dumps(
                    {
                        "category": "SYSTEM_TRANSIENT",
                        "recommended_action": "REDRIVE",
                        "confidence": "high",
                        "summary": "ok",
                        "reasoning": "ok",
                    }
                )
            }
        ]
    }
    monkeypatch.setattr(ba.boto3, "client", lambda _svc: DummyBedrock(payload))
    result = ba.handler({"message": {"id": "1"}}, None)
    assert result["llm"]["recommended_action"] == "TICKET"


def test_bedrock_adapter_invalid_action(monkeypatch):
    payload = {
        "content": [
            {
                "text": json.dumps(
                    {
                        "category": "SYSTEM_TRANSIENT",
                        "recommended_action": "DELETE",
                        "confidence": 0.9,
                        "summary": "ok",
                        "reasoning": "ok",
                    }
                )
            }
        ]
    }
    monkeypatch.setattr(ba.boto3, "client", lambda _svc: DummyBedrock(payload))
    result = ba.handler({"message": {"id": "1"}}, None)
    assert result["llm"]["recommended_action"] == "TICKET"


def test_bedrock_adapter_bedrock_api_error(monkeypatch):
    monkeypatch.setattr(ba.boto3, "client", lambda _svc: DummyBedrock({}, error=Exception("boom")))
    result = ba.handler({"message": {"id": "1"}}, None)
    assert result["llm"]["recommended_action"] == "TICKET"
