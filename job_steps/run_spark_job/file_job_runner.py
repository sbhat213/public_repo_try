from injector import inject
import traceback
from core.entities.file import FileExecution
from core.command.handler.update_file_status_handler import UpdateFileStatusCommandHandler
from core.command.update_file_status_command import UpdateFileStatusCommand
from core.entities.file_status import FileStatus
from job_steps.run_spark_job import STEP_NAME
# from job_steps import injector
from job_steps.run_spark_job.runner import Runner
from settings import Settings
from shared.livy.livy_client import LivyClient
from shared.logging.logger import Logger
from shared.utils.datetime import utc_now


class FileJobRunner(Runner):
    @inject
    def __init__(self, file_handler: UpdateFileStatusCommandHandler, settings: Settings, livy: LivyClient,
                 logger: Logger):
        self.settings = settings
        self.file_handler = file_handler
        self.livy = livy
        self.logger = logger

    def run(self, spark_state):
        logger = self.logger
        try:
            task = spark_state.task
            logger.info(f'In file job runner for model {task.model} and task_id {task.task_id}')
            logger.info(f'Calling Runner with arguments {spark_state.livyUrl} and {task.__dict__} and {type(task)}')

            response_id, livy_url = self.run_job(spark_state.livyUrl, task)
            update_file_status = UpdateFileStatusCommand(task.task_id, FileStatus.In_Progress.value, STEP_NAME,
                                                         utc_now(), last_success_date="",
                                                        execution_details=FileExecution(
                                                            response_id=response_id, error="", error_code="",
                                                            livy_url=livy_url, batch_id=""))
            self.file_handler.handle(update_file_status)
            logger.info(
                      f'Run Job Status updated to IN PROGRESS for model {task.model} and task_id  {task.task_id} ')

            return response_id, livy_url

        except Exception as e:
            logger.info(traceback.format_exc())
            logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
            raise e


