# DLQ Triage Sample

A runnable Python sample + **real AWS CDK infrastructure** for an end-to-end DLQ triage system: **classify -> decision -> action**, with guardrails.

![DLQ Triage architecture](DLQ_Triage_img.jpg)

## Contents

- `dlq_triage_sample.py` -- Runnable local sample
- `app.py`, `cdk.json`, `dlq_triage_infra/` -- CDK app
- `lambda/` -- DLQ triage, redrive, ticket, and producer lambdas

## Local sample

```bash
python dlq_triage_sample.py
```

## Prerequisites

- Python 3.11+ (for CDK deployment/runtime parity)
- AWS CLI configured with credentials
- CDK v2 installed (`npm install -g aws-cdk`)
- Bedrock model access enabled in your region (Claude 3.7 Sonnet availability varies by region)
- IAM permissions for Lambda, Step Functions, SQS, SNS, Bedrock

## Deploy the real infrastructure (CDK)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cdk synth
cdk bootstrap
cdk deploy
```

### What gets created

- SQS DLQ (source)
- DLQ triage Lambda (SQS-triggered)
- Producer Lambda (manual invoke to push test DLQ messages)
- Step Functions workflow with Bedrock classification + guardrails
- Redrive Lambda + Ticket Lambda
- SNS topic for triage outcomes

### Architecture flow (simple)

```
SQS DLQ -> Triage Lambda -> Step Functions -> [Bedrock Adapter -> Guardrails] -> Redrive/Ticket -> SNS
```

### Configuration

The default Bedrock model is Claude 3.7 Sonnet:

- `cdk.json` -> `context.model_id` = `anthropic.claude-3-7-sonnet-20250219-v1:0`

You can override via CDK context:

```bash
cdk deploy -c model_id=anthropic.claude-3-7-sonnet-20250219-v1:0 -c confidence_threshold=0.8
```

## Notes

- The Bedrock call expects the model to be enabled in your AWS account/region.
- Guardrails Lambda enforces age/attempt limits, token budget, and idempotency (placeholder check).
- Redrive and ticket lambdas are placeholders -- wire them to Kafka/SQS and Jira/ServiceNow as needed.
- The Step Function expects Claude to return **only JSON** with keys: `category`, `recommended_action`, `confidence`, `summary`, `reasoning` and uses only `REDRIVE` or `TICKET` as actions.
- Lambdas emit structured JSON logs and CloudWatch metrics (EMF) under the `DlqTriage` namespace, including per-category counts.
- Bedrock output is parsed and validated in a Lambda using Pydantic before guardrails run.
- AWS usage may incur costs (Step Functions, Lambda, Bedrock).
- Bedrock prompt input is truncated to 10k chars to limit prompt injection and cost.

## Cost Estimate (very rough)

- ~$0.50-$2.00 per 1000 messages (varies by region/model and payload size)

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

Tests cover: triage handler execution, Bedrock adapter parsing/fallback, guardrails logic (age/attempts/state/tokens).

## Quick smoke test after deploy

```bash
export AWS_REGION=us-east-1

# Set your account id once per shell
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "Using Account ID: $ACCOUNT_ID"

# Requires Step Functions read perms on this state machine:
# states:ListExecutions, states:DescribeExecution, states:GetExecutionHistory

# 1) Trigger producer with built-in sample payload
aws lambda invoke \
  --function-name DlqTriageStack-DlqProducerLambda15D95C31-GAu3oGCZ1t2Y \
  --payload '{}' /tmp/producer.json
cat /tmp/producer.json

# 2) Wait for processing
echo "Waiting 5 seconds for Step Functions execution..."
sleep 5

# 3) Check recent executions
echo "Recent executions:"
aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:us-east-1:${ACCOUNT_ID}:stateMachine:DlqTriageStateMachine0952112C-bXnfxX6bTZaO \
  --max-items 3 \
  --query 'executions[*].[name, status, startDate]' \
  --output table

# 4) Grab latest execution ARN
LATEST=$(aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:us-east-1:${ACCOUNT_ID}:stateMachine:DlqTriageStateMachine0952112C-bXnfxX6bTZaO \
  --max-items 1 \
  --query 'executions[0].executionArn' \
  --output text | grep -v "None")
echo "Latest execution ARN: $LATEST"

if [ -z "$LATEST" ] || [ "$LATEST" = "None" ]; then
  echo "No executions found (or access denied). Ensure you have Step Functions read permissions."
  exit 1
fi

# 5) Describe execution
echo "Execution details:"
aws stepfunctions describe-execution --execution-arn "$LATEST" \
  --query '{status: status, startDate: startDate, stopDate: stopDate}' \
  --output json

# 6) View execution history (optional)
echo "Execution flow:"
aws stepfunctions get-execution-history --execution-arn "$LATEST" \
  --max-results 10 \
  --reverse-order \
  --query 'events[*].[type, stateEnteredEventDetails.name]' \
  --output table

# 7) Check DLQ (should be empty on success)
echo "Checking DLQ for remaining messages:"
aws sqs receive-message \
  --queue-url https://sqs.us-east-1.amazonaws.com/${ACCOUNT_ID}/DlqTriageStack-DlqQueueFDA42DA7-qbOgIluwfrPR \
  --max-number-of-messages 5 \
  --wait-time-seconds 2
```

## Cleanup

```bash
cdk destroy
```

## Manual test message (Lambda invoke)

After deploy, you can manually invoke the producer Lambda with a test payload:

```bash
aws cloudformation describe-stacks \
  --stack-name DlqTriageStack \
  --query 'Stacks[0].Outputs[?OutputKey==`ProducerLambdaName`].OutputValue' \
  --output text
```

```bash
aws lambda invoke \
  --function-name <ProducerLambdaName_from_CDK_output> \
  --payload '{"message":{"correlationId":"sample-456","failureCategory":"DOWNSTREAM_TIMEOUT","errorMessage":"Timeout after 3 retries","timestamp":"2025-01-15T10:36:00Z","stateAtFailure":"FAILED","redriveAttempts":0}}' \
  out.json
```

## Troubleshooting

- Bedrock `AccessDeniedException`: enable the Claude model in the Bedrock console for your region.
- Step Function failures: check CloudWatch Logs for the Bedrock adapter or guardrails Lambda errors.
- IAM permission errors: verify Lambda, Step Functions, SQS, SNS, Bedrock permissions.
