from aws_cdk import core, aws_lambda as _lambda, aws_ec2 as ec2
from aws_cdk import aws_s3 as s3
import os
from config.config_parser import ConfigParser
from aws_cdk.aws_ec2 import Vpc, SecurityGroup


class LambdaFunction(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

    def create_lambda(self, function_name, role, layers, vpc, security_groups):
        lambda_function_config = LambdaFunction.lambda_function_config(function_name)
        vpc_config = {}

        if lambda_function_config['vpc']:
            vpc_config['vpc_subnets'] = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE)
            vpc_config['security_groups'] = security_groups
            vpc_config['vpc'] = vpc

        function = _lambda.Function(self, function_name,
                                    function_name=function_name,
                                    runtime=_lambda.Runtime.PYTHON_3_8,
                                    handler=lambda_function_config['handler_name'],
                                    role=role,
                                    code=_lambda.Code.from_asset(lambda_function_config['handler_path']),
                                    layers=layers,
                                    timeout=core.Duration.minutes(5), **vpc_config)
        return function

    @staticmethod
    def lambda_function_config(lambda_function_name):
        path = f"E:\\Shared\\Indegene\\aws_provision\\config\\"
        lambdas_config = ConfigParser.parse_config(path + "lambda_resource.json")
        return lambdas_config[lambda_function_name]
