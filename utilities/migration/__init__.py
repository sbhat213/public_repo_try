from aws_cloud.database.no_sql_db import NoSQLDB
from aws_cloud.database.dynamo import DynamoDB
from injector import Injector, singleton
from shared.redshift.redshift_sql_client import RedshiftSQLClient
from shared.logging.logger import Logger

from settings import Settings
injector = Injector()
injector.binder.bind(NoSQLDB, to=DynamoDB(Settings.AWS_CREDENTIALS), scope=singleton)
injector.binder.bind(RedshiftSQLClient, to=RedshiftSQLClient(Settings.REDSHIFT_CREDENTIALS, Logger), scope=singleton)

