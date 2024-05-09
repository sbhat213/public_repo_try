STEP_NAME = 'run_spark_job_step'

from injector import singleton
from job_steps import injector
from settings import Settings
from shared.logging.file_logger import FileLogger
from shared.logging.logger import Logger
from job_steps.dto.task_type import TaskType
from job_steps.run_spark_job.file_job_runner import FileJobRunner
from job_steps.run_spark_job.transformation_job_runner import TransformJobRunner



injector.binder.bind(TaskType.File.name, to=FileJobRunner, scope=singleton)
injector.binder.bind(TaskType.Transform.name, to=TransformJobRunner, scope=singleton)
injector.binder.bind(Logger, to=FileLogger, scope=singleton)

# injector.binder.bind(Logger, to=FileLogger.getInstance(Settings.AWS_CREDENTIALS, STEP_NAME), scope=singleton)
