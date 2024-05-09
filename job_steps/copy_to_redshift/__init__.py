STEP_NAME = 'copy_to_step'
from injector import singleton
from job_steps import injector
from settings import Settings
from shared.logging.file_logger import FileLogger
from shared.logging.logger import Logger
from aws_cloud.storage.file_storage import FileStorage
from aws_cloud.storage.s3 import AWSStorage
from shared.redshift.redshift_sql_client import RedshiftSQLClient
from job_steps.copy_to_redshift.copy_data import CopyData
from job_steps.copy_to_redshift.transformation_job_updater import TransformJobUpdater
from job_steps.dto.task_type import TaskType

injector.binder.bind(FileStorage, to=AWSStorage(Settings.AWS_CREDENTIALS), scope=singleton)
injector.binder.bind(RedshiftSQLClient, to=RedshiftSQLClient(Settings.REDSHIFT_CREDENTIALS, Logger), scope=singleton)
injector.binder.bind(Logger, to=FileLogger, scope=singleton)
injector.binder.bind(TaskType.Transform.name, to=TransformJobUpdater, scope=singleton)




# from shared.logging.aws_logger import AWSLogger
# injector.binder.bind(Logger, to=AWSLogger.getInstance(Settings.AWS_CREDENTIALS, STEP_NAME), scope=singleton)
# injector.binder.bind(CopyData, to=CopyData(FileStorage,RedshiftSQLClient,Settings), scope=singleton)