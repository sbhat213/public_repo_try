from aws_cdk import core, aws_lambda as _lambda, aws_s3 as s3, aws_iam as iam,aws_s3_notifications
from provision.provision_action.lambda_function import LambdaFunction
from provision.provision_action.iam_role import IAmRole
from config.config_parser import ConfigParser
from provision.resources.iam_job_role import JobIAmRole
from aws_cdk.aws_ec2 import SecurityGroup
from aws_cdk import aws_lambda_event_sources, aws_s3


class JobLambda(core.Stack):
    def __init__(self, scope: core.Construct, id: str, vpc=None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

    def createAll(self):
        self.lambda_functions = {}
        lambdas_config = JobLambda.lambda_function_config()
        functional_layer = _lambda.LayerVersion(self, "functional_dependecy",
                                     code=_lambda.Code.from_bucket(
                                         bucket=s3.Bucket.from_bucket_name(self, "functional-dependency",
                                                                           "idpmonjuvietljobs-sit"),
                                         key="SourceFeeds/1-deploy/function-dependency.zip"),
                                     compatible_runtimes=[_lambda.Runtime.PYTHON_3_8],
                                     layer_version_name="functional-dependecy-layer")

        redshift_layer = _lambda.LayerVersion(self, "redshift_functional_dependecy",
                                     code=_lambda.Code.from_bucket(
                                         bucket=s3.Bucket.from_bucket_name(self, "redshift-functional-dependecy",
                                                                           "idpmonjuvietljobs-sit"),
                                         key="SourceFeeds/1-deploy/redshift-function-dependency.zip"),
                                     compatible_runtimes=[_lambda.Runtime.PYTHON_3_8],
                                     layer_version_name="redshift-functional-dependency-layer")

        layers = [functional_layer,redshift_layer]
        bucket = s3.Bucket(self, "lambda-dependency", bucket_name="idpmonjuvietljobs-sourcefeeds-sit")

        dynamodb_sg_pvt_sg_id = self.security_group_map['dynamodb-sg-pvt'].attr_group_id

        livy_sg_pvt_sg_id = self.security_group_map['livy-sg-pvt'].attr_group_id

        security_groups = [SecurityGroup.from_security_group_id(self, "dynamodb_sg_pvt_sg_id", dynamodb_sg_pvt_sg_id),
                           SecurityGroup.from_security_group_id(self, "livy_sg_pvt_sg_id", livy_sg_pvt_sg_id)]

        for lambda_function_name in lambdas_config:
            lambda_role = self.iam_roles[lambdas_config[lambda_function_name]['role']]
            self.lambda_functions[lambda_function_name] = LambdaFunction.create_lambda(self, lambda_function_name,
                                                                                       lambda_role, layers, self.vpc,
                                                                                       security_groups)
            if (lambdas_config[lambda_function_name]['add_event']):
                notification = aws_s3_notifications.LambdaDestination(self.lambda_functions[lambda_function_name])
                bucket.add_event_notification(s3.EventType.OBJECT_CREATED, notification)

    @staticmethod
    def lambda_function_config():
        path = f"E:\\Shared\\Indegene\\aws_provision\\config\\"
        lambdas_config = ConfigParser.parse_config(path + "lambda_resource.json")
        return lambdas_config
