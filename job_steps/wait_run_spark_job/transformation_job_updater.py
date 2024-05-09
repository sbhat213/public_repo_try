from injector import inject
from core.command.handler.update_tj_status_handler import UpdateTJStatusHandler
from core.command.update_tJ_status_command import UpdateTJStatusCommand
from core.entities.job_status import JobStatus
from core.entities.error_code import ErrorCode
from settings import Settings
from job_steps.wait_run_spark_job.waiter import Waiter
from shared.livy.livy_client import LivyClient
from shared.logging.logger import Logger
from job_steps import injector
from shared.utils.datetime import utc_now,date_diffrence_hour,str_to_date,utc_now_string
from job_steps.wait_run_spark_job import STEP_NAME
from core.entities.transformation_job import JobExecution
from core.entities.job_type import JobType


class TransformJobUpdater(Waiter):
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

        if check_spark_state.job_type == JobType.Spark.value:
            response_type, response_status = self.check_status_job(check_spark_state.livy_url,
                                                                   check_spark_state.response_id)
            self.__updateSparkJobStatus(response_status, check_spark_state, logger, response_type)
            # return response_type, response_status
        elif check_spark_state.job_type == JobType.Rds.value:
            rds_status = self.check_redshift_status_job(check_spark_state.updated_on)
            self.__updateRdsJobStatus(rds_status, check_spark_state, logger)
            # return rds_status
        logger.info(check_spark_state.__dict__)
        return check_spark_state

    def __updateSparkJobStatus(self, response_status, check_spark_state, logger, response_type):

        if response_type:

            if response_status["state"] == "success":
                check_spark_state.status = JobStatus.Processed.value
                running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
                check_spark_state.updated_on = utc_now_string()
                check_spark_state.application_id = response_status["appId"]
                update_job_status = UpdateTJStatusCommand(check_spark_state.task_id, JobStatus.Processed.value,
                                                          STEP_NAME,
                                                          utc_now(), utc_now(), JobExecution(
                        response_id=check_spark_state.response_id, error="", error_code="",
                        livy_url=check_spark_state.livy_url, batch_id=response_status["appId"],
                        running_duration=running_duration))

                logger.info(
                    f'Run Job Status for TRANSFORMATION JOB updated to PROCESSED for model {check_spark_state.model} '
                    f'and task_id {check_spark_state.task_id} ')

                self.job_handler.handle(update_job_status)

            elif response_status["state"] == "dead":
                check_spark_state.status = JobStatus.Failed.value
                running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
                check_spark_state.updated_on = utc_now_string()
                check_spark_state.application_id = response_status["appId"]
                check_spark_state.error = str(response_status["log"])

                update_job_status = UpdateTJStatusCommand(check_spark_state.task_id, JobStatus.Failed.value, STEP_NAME,
                                                          utc_now(), "", JobExecution(
                        response_id=check_spark_state.response_id, error=str(response_status["log"]), error_code="",
                        livy_url=check_spark_state.livy_url, batch_id=response_status["appId"],
                        running_duration=running_duration))
                logger.info(
                    f"Run Job Status for TRANSFORMATION JOB updated to FAILED for model {check_spark_state.model} and "
                    f"task_id {check_spark_state.task_id} ")

                self.job_handler.handle(update_job_status)

            elif response_status["state"] == "running" or response_status["state"] == "starting" :

                check_spark_state.status = JobStatus.In_Progress.value
                # check_spark_state.updated_on = utc_now_string()
                check_spark_state.application_id = response_status["appId"]
                running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
                update_job_status = UpdateTJStatusCommand(check_spark_state.task_id, JobStatus.In_Progress.value,
                                                          STEP_NAME,
                                                          utc_now(), "", JobExecution(
                        response_id=check_spark_state.response_id, error="", error_code="",
                        livy_url=check_spark_state.livy_url, batch_id=response_status["appId"],
                        running_duration=running_duration))

                logger.info(
                    f"Run Job Status for TRANSFORMATION JOB updated to IN PROGRESS for model {check_spark_state.model} and "
                    f"task_id {check_spark_state.task_id} ")

                self.job_handler.handle(update_job_status)


        else:
            check_spark_state.status = JobStatus.Failed.value
            running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
            check_spark_state.updated_on = utc_now_string()
            check_spark_state.error_code = ErrorCode.bad_response.value
            check_spark_state.error = response_status["msg"]
            update_job_status = UpdateTJStatusCommand(check_spark_state.task_id, JobStatus.Failed.value, STEP_NAME,
                                                      utc_now(), "", JobExecution(
                    response_id=check_spark_state.response_id, error=response_status["msg"],
                    error_code=ErrorCode.bad_response.value,
                    livy_url=check_spark_state.livy_url, batch_id="", running_duration=running_duration))
            logger.info(
                f"Run Job Status for TRANSFORMATION JOB updated to FAILED DUE TO PAGE NOT FOUND EXCEPTION for model {check_spark_state.model} and "
                f"task_id {check_spark_state.task_id} "
                f"Error code updated to {ErrorCode.bad_response.value}")
            self.job_handler.handle(update_job_status)

    def __updateRdsJobStatus(self, rds_status, check_spark_state, logger):

        if not rds_status:
            check_spark_state.status = JobStatus.Failed.value
            running_duration = date_diffrence_hour(utc_now(), str_to_date(check_spark_state.updated_on))
            check_spark_state.updated_on = utc_now_string()
            check_spark_state.error_code = ErrorCode.rds_timeout.value
            check_spark_state.error = ErrorCode.rds_timeout.value
            update_job_status = UpdateTJStatusCommand(check_spark_state.task_id, JobStatus.Failed.value, STEP_NAME,
                                                      utc_now(), "", JobExecution(
                    response_id="", error="", error_code=ErrorCode.rds_timeout.value,
                    livy_url="", batch_id="", running_duration=running_duration))
            logger.info(
                f"Run Job Status for REDSHIFT JOB updated to FAILED for model {check_spark_state.model} and "
                f"task_id {check_spark_state.task_id} "
                f"Error code updated to {ErrorCode.rds_timeout.value}"
            )
            self.job_handler.handle(update_job_status)
