from injector import inject
from core.command.handler.update_tj_status_handler import UpdateTJStatusHandler
from core.command.update_tJ_status_command import UpdateTJStatusCommand
from core.entities.job_status import JobStatus
from settings import Settings
from job_steps.run_spark_job.runner import Runner
from shared.livy.livy_client import LivyClient
from shared.logging.logger import Logger
# from job_steps import injector
from job_steps.run_spark_job import STEP_NAME
from shared.utils.datetime import utc_now
from core.entities.transformation_job import JobExecution


class TransformJobRunner(Runner):
    @inject
    def __init__(self, job_handler: UpdateTJStatusHandler, settings: Settings, livy: LivyClient, logger: Logger):
        self.settings = settings
        self.job_handler = job_handler
        self.livy = livy
        self.logger = logger

    def run(self, spark_state):
        logger = self.logger
        task = spark_state.task
        logger.info(f'In Transformation job runner for model {task.model} and task_id {task.task_id}')
        response_id, livy_url = self.run_job(spark_state.livyUrl, task)
        # update_jobexecution_status = UpdateJEStatusCommand(response_id=response_id, error="", error_code="", livy_url=livy_url, batch_id="")

        update_job_status = UpdateTJStatusCommand(job_id=task.task_id, status=JobStatus.In_Progress.value,
                                                  updated_by=STEP_NAME,
                                                  updated_on=utc_now(), last_success_date="",
                                                  execution_details=JobExecution(
                                                      response_id=response_id, error="", error_code="",
                                                      livy_url=livy_url, batch_id=""))

        logger.info(
            f'Run Job Status for TRANSFORMATION JOB updated to IN PROGRESS for model {task.model} and  task_id {task.task_id}')
        self.job_handler.handle(update_job_status)

        return response_id, livy_url
