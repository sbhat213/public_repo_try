STEP_NAME = 'wait_run_spark_job_step'
from injector import singleton
from job_steps import injector
from settings import Settings
from shared.logging.file_logger import FileLogger
from shared.logging.logger import Logger
from job_steps.dto.task_type import TaskType
from job_steps.wait_run_spark_job.file_job_updater import FileJobUpdater
from job_steps.wait_run_spark_job.transformation_job_updater import TransformJobUpdater

injector.binder.bind(TaskType.File.name, to=FileJobUpdater, scope=singleton)
injector.binder.bind(TaskType.Transform.name, to=TransformJobUpdater, scope=singleton)
injector.binder.bind(Logger, to=FileLogger, scope=singleton)
