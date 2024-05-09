#!/usr/bin/env python3

from aws_cdk import core

from aws_provision.aws_provision_stack import AwsProvisionStack


app = core.App()
AwsProvisionStack(app, "aws-provision")

app.synth()
