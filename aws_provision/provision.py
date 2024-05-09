#!/usr/bin/env python3
from aws_cdk import core, aws_ec2 as ec2
from provision.resources.vpc_stack import VpcStack
from provision.resources.job_security_group import JobSecurityGroup
from provision.resources.iam_job_role import JobIAmRole
from provision.resources.job_lambda import JobLambda
from provision.resources.step_function import StepFunctionJob


class Provision(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

    def create(self):
        VpcStack.create_vpc(self, "IDP-Pvt-vpc")
        JobSecurityGroup.createAll(self)
        JobIAmRole.createAll(self)
        JobLambda.createAll(self)
        # print(self.lambda_functions)

        StepFunctionJob.createAll(self)


if __name__ == "__main__":
    app = core.App()
    Provision(app, "aws-provision").create()
    app.synth()
