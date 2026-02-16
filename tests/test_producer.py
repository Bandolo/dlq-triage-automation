from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "lambda"))

import producer_handler as ph


class DummySqs:
    def __init__(self):
        self.sent = []

    def send_message(self, QueueUrl, MessageBody):
        self.sent.append({"QueueUrl": QueueUrl, "MessageBody": MessageBody})
        return {"MessageId": "1"}


def test_producer_sends_message(monkeypatch):
    dummy = DummySqs()
    monkeypatch.setattr(ph.boto3, "client", lambda service: dummy)
    monkeypatch.setenv("DLQ_QUEUE_URL", "https://example.com/queue")

    result = ph.handler({"message": {"correlationId": "x"}}, None)

    assert result["status"] == "sent"
    assert dummy.sent
    assert dummy.sent[0]["QueueUrl"] == "https://example.com/queue"
