from injector import inject
from core.command.handler.update_tj_status_handler import UpdateTJStatusHandler
from core.command.update_tJ_status_command import UpdateTJStatusCommand
from core.entities.job_status import JobStatus
from settings import Settings
from shared.logging.logger import Logger
from job_steps.copy_to_redshift import STEP_NAME
from shared.utils.datetime import utc_now
from core.entities.transformation_job import JobExecution


class TransformJobUpdater:
    @inject
    def __init__(self, job_handler: UpdateTJStatusHandler, settings: Settings, logger: Logger):
        self.settings = settings
        self.job_handler = job_handler
        self.logger = logger

    def update(self, spark_state, job_status):
        logger = self.logger
        task = spark_state.task
        logger.info(f'In Transformation job runner for model {task.model} and task_id {task.task_id}')
        if job_status == JobStatus.In_Progress.value:
            update_job_status = UpdateTJStatusCommand(job_id=task.task_id, status=JobStatus.In_Progress.value,
                                                      updated_by=STEP_NAME,
                                                      updated_on=utc_now(), last_success_date="",
                                                      execution_details=JobExecution(
                                                          response_id=0, error="", error_code="",
                                                          livy_url="", batch_id=""))
        elif job_status == JobStatus.Processed.value:
            update_job_status = UpdateTJStatusCommand(job_id=task.task_id, status=JobStatus.Processed.value,
                                                      updated_by=STEP_NAME,
                                                      updated_on=utc_now(), last_success_date=utc_now(),
                                                      execution_details=JobExecution(
                                                          response_id=0, error="", error_code="",
                                                          livy_url="", batch_id=""))

        logger.info(
            f'Run Job Status for TRANSFORMATION JOB updated to IN PROGRESS for model {task.model} and  task_id {task.task_id}')
        self.job_handler.handle(update_job_status)
