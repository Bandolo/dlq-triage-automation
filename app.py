#!/usr/bin/env python3
import aws_cdk as cdk

from dlq_triage_infra.stack import DlqTriageStack


app = cdk.App()
DlqTriageStack(app, "DlqTriageStack")
app.synth()
