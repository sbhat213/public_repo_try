from job_steps.wait_run_spark_job import STEP_NAME
from injector import inject
from shared.utils.datetime import str_to_date, date_diffrence_hour
from core.entities.file import FileExecution
from shared.utils.datetime import utc_now
from shared.utils.datetime import utc_now_string
from core.command.handler.update_file_status_handler import UpdateFileStatusCommandHandler
from core.command.update_file_status_command import UpdateFileStatusCommand
from core.entities.file_status import FileStatus
from core.entities.error_code import ErrorCode
from settings import Settings
from job_steps.wait_run_spark_job.waiter import Waiter
from shared.livy.livy_client import LivyClient
from shared.logging.logger import Logger


class FileJobUpdater(Waiter):
    @inject
    def __init__(self, file_handler: UpdateFileStatusCommandHandler, settings: Settings, livy: LivyClient,
                 logger: Logger):
        self.settings = settings
        self.file_handler = file_handler
        self.livy = livy
        self.logger = logger

    def run(self, check_spark_state):
        logger = self.logger

        logger.info("In Transformation job runner for model {} and task_id {}".format(check_spark_state.model,
                                                                                      check_spark_state.task_id))
        response_type, response_status = self.check_status_job(check_spark_state.livy_url,
                                                               check_spark_state.response_id)
        if response_type:
            if response_status["state"] == "success":
                check_spark_state.status = FileStatus.Processed.value
                running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
                check_spark_state.updated_on = utc_now_string()
                check_spark_state.application_id = response_status["appId"]

                update_job_status = UpdateFileStatusCommand(check_spark_state.task_id, FileStatus.Processed.value,
                                                            STEP_NAME, utc_now(), utc_now(),
                                                            execution_details=FileExecution(
                                                                response_id=check_spark_state.response_id, error="",
                                                                error_code="",
                                                                livy_url=check_spark_state.livy_url,
                                                                batch_id=response_status["appId"],
                                                                running_duration=running_duration
                                                            ))
                logger.info(
                    f'Run Job Status for FILE JOB updated to PROCESSED for model {check_spark_state.model} and  '
                    f'task_id {check_spark_state.task_id} ')

                self.file_handler.handle(update_job_status)
            elif response_status["state"] == "dead":
                check_spark_state.status = FileStatus.Failed.value
                running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
                check_spark_state.updated_on = utc_now_string()
                check_spark_state.application_id = response_status["appId"]
                check_spark_state.error = str(response_status["log"])
                update_job_status = UpdateFileStatusCommand(check_spark_state.task_id, FileStatus.Failed.value,
                                                            STEP_NAME, utc_now(), "", execution_details=FileExecution(
                        response_id=check_spark_state.response_id, error=str(response_status["log"]), error_code="",
                        livy_url=check_spark_state.livy_url, batch_id=response_status["appId"],
                        running_duration=running_duration))

                logger.info(
                    f'Run Job Status for FILE JOB  updated to DEAD for model {check_spark_state.model} and '
                    f'task_id {check_spark_state.task_id} ')

                self.file_handler.handle(update_job_status)


            elif response_status["state"] == "running" or response_status["state"] == "starting":
                check_spark_state.status = FileStatus.In_Progress.value
                running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
                # check_spark_state.updated_on = utc_now_string()
                check_spark_state.application_id = response_status["appId"]
                update_job_status = UpdateFileStatusCommand(check_spark_state.task_id, FileStatus.In_Progress.value,
                                                            STEP_NAME, utc_now(), "", execution_details=FileExecution(
                        response_id=check_spark_state.response_id, error=str(response_status["log"]), error_code="",
                        livy_url=check_spark_state.livy_url, batch_id=response_status["appId"],
                        running_duration=running_duration))

                logger.info(
                f'Run Job Status for FILE JOB  updated to IN PROGRESS for model {check_spark_state.model} and '
                f'task_id {check_spark_state.task_id} ')
                self.file_handler.handle(update_job_status)
        else:
            check_spark_state.status = FileStatus.Failed.value
            running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
            check_spark_state.updated_on = utc_now_string()
            check_spark_state.error_code = ErrorCode.bad_response.value
            check_spark_state.error = response_status["msg"]
            update_job_status = UpdateFileStatusCommand(check_spark_state.task_id, FileStatus.Failed.value, STEP_NAME,
                                                        utc_now(), "", FileExecution(
                    response_id=check_spark_state.response_id, error=response_status["msg"],
                    error_code=ErrorCode.bad_response.value,
                    livy_url=check_spark_state.livy_url, batch_id="", running_duration=running_duration))
            logger.info(
                f"Run Job Status for TRANSFORMATION JOB updated to FAILED DUE TO PAGE NOT FOUND "
                f"EXCEPTION for model {check_spark_state.model} and "
                f"task_id {check_spark_state.task_id} "
                f"Error code updated to {ErrorCode.bad_response.value}")
            self.file_handler.handle(update_job_status)

        return check_spark_state
