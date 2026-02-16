import json
import os
from typing import Any, Dict, Literal

import boto3
from pydantic import BaseModel, ValidationError, confloat


class TriageOutput(BaseModel):
    category: str
    recommended_action: Literal["REDRIVE", "TICKET"]
    confidence: confloat(ge=0.0, le=1.0)
    summary: str
    reasoning: str


def _fallback_llm(reason: str) -> Dict[str, Any]:
    return {
        "category": "UNKNOWN",
        "recommended_action": "TICKET",
        "confidence": 0.0,
        "summary": "Invalid model response",
        "reasoning": reason,
    }


def handler(event, _context):
    message: Dict[str, Any] = event.get("message", {})
    model_id = os.getenv("MODEL_ID", "anthropic.claude-3-7-sonnet-20250219-v1:0")

    bedrock_region = os.getenv("BEDROCK_REGION") or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    try:
        client = boto3.client("bedrock-runtime", region_name=bedrock_region)
    except TypeError:
        # Tests monkeypatch boto3.client with a lambda that only takes the service name
        client = boto3.client("bedrock-runtime")

    message_str = json.dumps(message)
    if len(message_str) > 10000:
        message_str = message_str[:10000] + "... [truncated]"

    prompt = (
        "Return ONLY JSON with keys: category, recommended_action, confidence, summary, reasoning.\n"
        "- recommended_action must be REDRIVE or TICKET\n"
        "- confidence must be a number between 0 and 1\n"
        "- summary: 1 sentence\n"
        "- reasoning: 1-2 sentences\n"
        "No extra text.\n\n"
        "DLQ event:\n"
        f"{message_str}"
    )

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
    }

    try:
        resp = client.invoke_model(
            ModelId=model_id,
            ContentType="application/json",
            Accept="application/json",
            Body=json.dumps(payload),
        )
        body = json.loads(resp["Body"].read().decode("utf-8"))
        text = body.get("content", [{}])[0].get("text", "")
    except Exception:
        print(json.dumps({"level": "ERROR", "message": "Bedrock invoke failed"}))
        llm = _fallback_llm("Bedrock invoke failed")
        return {"message": message, "llm": llm}

    try:
        parsed = json.loads(text)
        if hasattr(TriageOutput, "model_validate"):
            triage = TriageOutput.model_validate(parsed)
        else:  # Pydantic v1 fallback
            triage = TriageOutput.parse_obj(parsed)
        llm = triage.model_dump() if hasattr(triage, "model_dump") else triage.dict()
    except (json.JSONDecodeError, ValidationError):
        print(json.dumps({"level": "WARN", "message": "Bedrock output invalid"}))
        llm = _fallback_llm("Failed to parse/validate model output")

    return {"message": message, "llm": llm}
