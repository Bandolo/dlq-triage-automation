from __future__ import annotations

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import Duration
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_lambda_event_sources as lambda_events
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_sns as sns
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class DlqTriageStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        model_id = self.node.try_get_context("model_id") or "anthropic.claude-3-sonnet-20240229-v1:0"
        bedrock_region = self.node.try_get_context("bedrockRegion") or "us-east-1"
        confidence_threshold = float(self.node.try_get_context("confidence_threshold") or 0.8)

        dlq_queue = sqs.Queue(
            self,
            "DlqQueue",
            visibility_timeout=Duration.seconds(60),
        )

        notify_topic = sns.Topic(self, "DlqTriageNotifications")

        lambda_dir = Path(__file__).resolve().parent.parent / "lambda"

        triage_lambda = _lambda.Function(
            self,
            "DlqTriageLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="triage_handler.handler",
            code=_lambda.Code.from_asset(str(lambda_dir)),
            timeout=Duration.seconds(30),
            environment={
                "STATE_MACHINE_ARN": "PLACEHOLDER",
            },
        )

        redrive_lambda = _lambda.Function(
            self,
            "DlqRedriveLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="redrive_handler.handler",
            code=_lambda.Code.from_asset(str(lambda_dir)),
            timeout=Duration.seconds(30),
        )

        ticket_lambda = _lambda.Function(
            self,
            "DlqTicketLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="ticket_handler.handler",
            code=_lambda.Code.from_asset(str(lambda_dir)),
            timeout=Duration.seconds(30),
        )

        bedrock_adapter_lambda = _lambda.Function(
            self,
            "DlqBedrockAdapterLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="bedrock_adapter.handler",
            timeout=Duration.seconds(30),
            environment={
                "MODEL_ID": model_id,
                "BEDROCK_REGION": bedrock_region,
            },
            code=_lambda.Code.from_asset(str(lambda_dir)),
        )

        guardrails_lambda = _lambda.Function(
            self,
            "DlqGuardrailsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="guardrails_handler.handler",
            code=_lambda.Code.from_asset(str(lambda_dir)),
            timeout=Duration.seconds(30),
        )

        producer_lambda = _lambda.Function(
            self,
            "DlqProducerLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="producer_handler.handler",
            code=_lambda.Code.from_asset(str(lambda_dir)),
            timeout=Duration.seconds(30),
            environment={
                "DLQ_QUEUE_URL": dlq_queue.queue_url,
            },
        )

        # Step Functions: Bedrock adapter -> guardrails choice -> action -> SNS notify
        bedrock_task = tasks.LambdaInvoke(
            self,
            "BedrockAdapter",
            lambda_function=bedrock_adapter_lambda,
            payload=sfn.TaskInput.from_object({"message.$": "$.message"}),
            result_path="$.bedrock_result",
        )
        bedrock_task.add_retry(
            max_attempts=2,
            interval=Duration.seconds(2),
            backoff_rate=2.0,
        )

        guardrails_task = tasks.LambdaInvoke(
            self,
            "GuardrailsLambda",
            lambda_function=guardrails_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "message.$": "$.bedrock_result.Payload.message",
                    "llm.$": "$.bedrock_result.Payload.llm",
                    "max_age_days": 2,
                    "max_redrive_attempts": 2,
                    "max_token_estimate": 2000,
                }
            ),
            result_path="$.guardrails_result",
        )

        redrive_task = tasks.LambdaInvoke(
            self,
            "RedriveLambda",
            lambda_function=redrive_lambda,
            payload=sfn.TaskInput.from_object(
                {"message.$": "$.guardrails_result.Payload.message", "llm.$": "$.guardrails_result.Payload.llm"}
            ),
            result_path="$.redrive",
        )
        redrive_task.add_retry(
            max_attempts=2,
            interval=Duration.seconds(2),
            backoff_rate=2.0,
        )

        ticket_task = tasks.LambdaInvoke(
            self,
            "TicketLambda",
            lambda_function=ticket_lambda,
            payload=sfn.TaskInput.from_object(
                {"message.$": "$.guardrails_result.Payload.message", "llm.$": "$.guardrails_result.Payload.llm"}
            ),
            result_path="$.ticket",
        )
        ticket_task.add_retry(
            max_attempts=2,
            interval=Duration.seconds(2),
            backoff_rate=2.0,
        )

        notify_task = tasks.SnsPublish(
            self,
            "Notify",
            topic=notify_topic,
            message=sfn.TaskInput.from_object(
                {
                    "correlationId.$": "$.guardrails_result.Payload.message.correlationId",
                    "recommended_action.$": "$.guardrails_result.Payload.llm.recommended_action",
                    "category.$": "$.guardrails_result.Payload.llm.category",
                    "summary.$": "$.guardrails_result.Payload.llm.summary",
                    "allow_redrive.$": "$.guardrails_result.Payload.guardrails.allow_redrive",
                    "guardrail_reasons.$": "$.guardrails_result.Payload.guardrails.reasons",
                }
            ),
            subject="DLQ triage outcome",
        )

        decision = sfn.Choice(self, "Decision")
        decision.when(
            sfn.Condition.and_(
                sfn.Condition.string_equals("$.bedrock_result.Payload.llm.recommended_action", "REDRIVE"),
                sfn.Condition.number_greater_than_equals("$.bedrock_result.Payload.llm.confidence", confidence_threshold),
                sfn.Condition.boolean_equals("$.guardrails_result.Payload.guardrails.allow_redrive", True),
            ),
            redrive_task.next(notify_task),
        )
        decision.otherwise(ticket_task.next(notify_task))

        workflow = sfn.StateMachine(
            self,
            "DlqTriageStateMachine",
            definition=bedrock_task.next(guardrails_task).next(decision),
            timeout=Duration.minutes(2),
        )

        # Wire state machine ARN into triage lambda
        triage_lambda.add_environment("STATE_MACHINE_ARN", workflow.state_machine_arn)

        # Allow triage lambda to start executions
        workflow.grant_start_execution(triage_lambda)

        # Allow Step Functions to invoke lambdas and publish SNS
        redrive_lambda.grant_invoke(workflow.role)
        ticket_lambda.grant_invoke(workflow.role)
        guardrails_lambda.grant_invoke(workflow.role)
        bedrock_adapter_lambda.grant_invoke(workflow.role)
        notify_topic.grant_publish(workflow.role)

        # Allow Bedrock adapter to call Bedrock
        bedrock_adapter_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel"],
                resources=["*"],
            )
        )

        # Event source: SQS DLQ -> triage lambda
        triage_lambda.add_event_source(
            lambda_events.SqsEventSource(
                dlq_queue,
                batch_size=1,
            )
        )

        # Allow producer lambda to send test messages into DLQ
        dlq_queue.grant_send_messages(producer_lambda)

        # Outputs
        cdk.CfnOutput(self, "DlqQueueUrl", value=dlq_queue.queue_url)
        cdk.CfnOutput(self, "StateMachineArn", value=workflow.state_machine_arn)
        cdk.CfnOutput(self, "SnsTopicArn", value=notify_topic.topic_arn)
        cdk.CfnOutput(self, "ProducerLambdaName", value=producer_lambda.function_name)
