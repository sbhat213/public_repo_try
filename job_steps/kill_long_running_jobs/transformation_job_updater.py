from injector import inject
from core.command.handler.update_tj_status_handler import UpdateTJStatusHandler
from core.command.update_tJ_status_command import UpdateTJStatusCommand
from core.entities.job_status import JobStatus
from core.entities.error_code import ErrorCode
from settings import Settings
from job_steps.kill_long_running_jobs.kill_session import Killsession
from shared.livy.livy_client import LivyClient
from shared.logging.logger import Logger
from job_steps import injector
from shared.utils.datetime import utc_now, date_diffrence_hour, str_to_date, utc_now_string
from job_steps.kill_long_running_jobs import STEP_NAME
from core.entities.transformation_job import JobExecution
from core.entities.job_type import JobType


class TransformJobUpdater(Killsession):
    @inject
    def __init__(self, job_handler: UpdateTJStatusHandler, settings: Settings, livy: LivyClient, logger: Logger):
        self.settings = settings
        self.job_handler = job_handler
        self.livy = livy
        self.logger = logger

    def run(self, check_spark_state):
        logger = injector.get(Logger)

        logger.info("In Transformation job updater for model {} and task_id {}".format(check_spark_state.model,
                                                                                       check_spark_state.task_id))
        running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
        if running_duration >= Settings.Schedule["long_running_hours"]:
            if check_spark_state.job_type == JobType.Spark.value:
                response_status = self.kill_job(check_spark_state.livy_url, check_spark_state.response_id)
                self.__updateSparkJobStatus(check_spark_state, running_duration, logger)

        logger.info(check_spark_state.__dict__)
        return check_spark_state

    def __updateSparkJobStatus(self, check_spark_state, running_duration, logger):

        check_spark_state.status = JobStatus.Failed.value
        check_spark_state.updated_on = utc_now_string()
        check_spark_state.error_code = ErrorCode.long_running_job.value
        check_spark_state.error = ErrorCode.long_running_job.value
        update_job_status = UpdateTJStatusCommand(check_spark_state.task_id, JobStatus.Failed.value, STEP_NAME,
                                                  utc_now(), "", JobExecution(
                response_id=check_spark_state.response_id, error=ErrorCode.long_running_job.value,
                error_code=ErrorCode.long_running_job.value,
                livy_url=check_spark_state.livy_url, batch_id="", running_duration=running_duration))
        logger.info(
            f"Run Job Status for TRANSFORMATION JOB updated to FAILED DUE TO LONG RUNNING JOB for model "
            f"{check_spark_state.model} and "
            f"task_id {check_spark_state.task_id} "
            f"Error code updated to {ErrorCode.bad_response.value}")
        self.job_handler.handle(update_job_status)
