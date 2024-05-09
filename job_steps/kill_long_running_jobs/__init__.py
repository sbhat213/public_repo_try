STEP_NAME = 'kill_long_running_job_step'
from injector import singleton
from job_steps import injector
from settings import Settings
from shared.logging.file_logger import FileLogger
from shared.logging.logger import Logger
from job_steps.dto.task_type import TaskType
from job_steps.kill_long_running_jobs.file_job_updater import FileJobUpdater
from job_steps.kill_long_running_jobs.transformation_job_updater import TransformJobUpdater

injector.binder.bind(TaskType.File.name, to=FileJobUpdater, scope=singleton)
injector.binder.bind(TaskType.Transform.name, to=TransformJobUpdater, scope=singleton)
injector.binder.bind(Logger, to=FileLogger, scope=singleton)
