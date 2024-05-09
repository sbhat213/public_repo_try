import boto3
from boto3.exceptions import ResourceNotExistsError
from pynamodb.constants import PAY_PER_REQUEST_BILLING_MODE
from pynamodb.models import Model

from aws_cloud.database.no_sql_db import NoSQLDB


class DynamoDB(NoSQLDB):
    def __init__(self, settings):
        if settings is None:
            self.client = boto3.client(
                "dynamodb"
            )
        else:
            self.client = boto3.client(
                "dynamodb",
                aws_access_key_id=settings["access_key_id"],
                aws_secret_access_key=settings["secret_access_key"],
                region_name=settings["region"]
            )
        super(DynamoDB, self).__init__()

    # Here we are passing the pynamodb's model object and synthesizing its creation feature
    # We are not using the boto for dynamodb table creation as it provides an easy attirbute controlling
    def create_table(self, model: Model, billing_mode: str = PAY_PER_REQUEST_BILLING_MODE, wait=True):
        model.create_table(billing_mode=billing_mode, wait=wait)

    def delete_table(self, table_name):
        try:
            self.client.delete_table(TableName=table_name)
            waiter = self.client.get_waiter('table_not_exists')
            waiter.wait(TableName=table_name)
        except ResourceNotExistsError as ex:
            return

    def truncate_table(self, model: Model):
        self.delete_table(model.Meta.table_name)
        self.create_table(model)
