from injector import inject, singleton
from core.entities.job_status import JobStatus
from core.command.update_tJ_status_command import UpdateTJStatusCommand
from core.repository.transformation_job_repository import TransformationJobRepository
from shared.logging.logger import Logger


@singleton
class UpdateTJStatusHandler:
    @inject
    def __init__(self, tj_repository: TransformationJobRepository, logger: Logger):
        self.tj_repository = tj_repository
        self.logger = logger

    def handle(self, command: UpdateTJStatusCommand):
        logger = self.logger
        job = self.tj_repository.get(command.job_id)
        job.status = command.status
        job.updated_on = command.updated_on
        job.updated_by = command.updated_by
        job.last_execution_detail = command.execution_details
        if command.status == JobStatus.Processed.value:
            job.retry_count = 0
            job.last_success_date = command.last_success_date
            dependent_jobs = job.dependent_jobs
            logger.info(f'Dependent jobs are {dependent_jobs}')
            if len(dependent_jobs) >= 1:
                for item_id in dependent_jobs:
                    dependent_job = self.tj_repository.get(item_id)
                    dependent_job.status = JobStatus.Ready_To_Schedule.value
                    self.tj_repository.add(dependent_job)
        if command.status == JobStatus.Failed.value:
            job.retry_count = job.retry_count + 1
        # if command.status == JobStatus.Failed.value & job.retry_count == 3 :
        #     job.retry_count = 0
        #     job.status = JobStatus.Created.value

        self.tj_repository.add(job)
