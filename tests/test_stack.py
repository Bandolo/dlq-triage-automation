from pathlib import Path
import os
import sys
import shutil

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

if shutil.which("node") is None:
    pytest.skip("node is required for aws_cdk jsii runtime", allow_module_level=True)

home_cache = Path.home() / "Library" / "Caches"
if not home_cache.exists() or not os.access(home_cache, os.W_OK):
    pytest.skip("jsii cache directory is not writable in this environment", allow_module_level=True)

os.environ.setdefault("JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION", "1")

aws_cdk = pytest.importorskip("aws_cdk")
from aws_cdk import assertions

from dlq_triage_infra.stack import DlqTriageStack


def test_state_machine_contains_guardrails_and_bedrock():
    app = aws_cdk.App()
    stack = DlqTriageStack(app, "TestStack")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
    sm = template.find_resources("AWS::StepFunctions::StateMachine")
    definition = next(iter(sm.values()))["Properties"]["DefinitionString"]

    # Basic smoke checks on the ASL definition
    assert "CallBedrock" in definition
    assert "GuardrailsLambda" in definition
    assert "Decision" in definition
