import boto3

from aws_cloud.function.function_detail import FunctionDetail
from aws_cloud.function.severless import Serverless


class AWSLambda(Serverless):
    def __init__(self, settings):
        self.client = boto3.client(
            "lambda",
            aws_access_key_id=settings["access_key_id"],
            aws_secret_access_key=settings["secret_access_key"],
            region_name=settings["region"]
        )

    def update_function_code(self, detail: FunctionDetail):
        response = self.client.update_function_code(
            FunctionName=detail.name,
            S3Bucket=detail.code_location,
            S3Key=detail.file,
            Publish=True
        )
        return response
