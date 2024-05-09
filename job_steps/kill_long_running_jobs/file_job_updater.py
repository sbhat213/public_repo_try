from job_steps.kill_long_running_jobs import STEP_NAME
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
from job_steps.kill_long_running_jobs.kill_session import Killsession
from shared.livy.livy_client import LivyClient
from shared.logging.logger import Logger
from core.entities.job_type import JobType


class FileJobUpdater(Killsession):
    @inject
    def __init__(self, file_handler: UpdateFileStatusCommandHandler, settings: Settings, livy: LivyClient,
                 logger: Logger):
        self.settings = settings
        self.file_handler = file_handler
        self.livy = livy
        self.logger = logger

    def run(self, check_spark_state):
        logger = self.logger

        logger.info(f"In File job Updater after KILL JOB  for model {check_spark_state.model} \n and "
                    f"task_id {check_spark_state.task_id}")
        running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
        if running_duration >= Settings.Schedule["long_running_hours"]:
            if check_spark_state.job_type == JobType.Spark.value:
                response_status = self.kill_job(check_spark_state.livy_url, check_spark_state.response_id)
                self.__updateSparkJobStatus(check_spark_state,running_duration, logger)

        logger.info(check_spark_state.__dict__)
        return check_spark_state

    def __updateSparkJobStatus(self, check_spark_state,running_duration, logger):
        check_spark_state.status = FileStatus.Failed.value
        check_spark_state.updated_on = utc_now_string()
        check_spark_state.error_code = ErrorCode.long_running_job.value
        check_spark_state.error = ErrorCode.long_running_job.value
        update_job_status = UpdateFileStatusCommand(check_spark_state.task_id, FileStatus.Failed.value, STEP_NAME,
                                                    utc_now(), "", FileExecution(
                response_id=check_spark_state.response_id, error=ErrorCode.long_running_job.value,
                error_code=ErrorCode.long_running_job.value,
                livy_url=check_spark_state.livy_url, batch_id="", running_duration=running_duration))
        logger.info(
            f"Run Job Status for TRANSFORMATION JOB updated to FAILED DUE TO LONG RUNNING DURATION"
            f"EXCEPTION for model {check_spark_state.model} and "
            f"task_id {check_spark_state.task_id} "
            f"Error code updated to {ErrorCode.long_running_job.value}")

        self.file_handler.handle(update_job_status)

        return check_spark_state
