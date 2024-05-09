from injector import singleton

from job_steps import injector
from settings import Settings
from shared.logging.aws_logger import AWSLogger
from shared.logging.logger import Logger

STEP_NAME = 'send_failure_notification'
injector.binder.bind(Logger, to=AWSLogger.getInstance(Settings.AWS_CREDENTIALS, STEP_NAME), scope=singleton)
