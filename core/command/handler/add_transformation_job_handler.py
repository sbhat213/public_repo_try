from datetime import datetime
from shared.utils.datetime import utc_now
from injector import inject, singleton
from core.command.add_transformation_job_command import AddTransformationJobCommand
from core.entities.transformation_job import TransformationJob
from core.repository.transformation_job_repository import TransformationJobRepository


@singleton
class AddTransformationHandler:
    @inject
    def __init__(self, transformation_job_repository: TransformationJobRepository):
        self.transformation_job_repository = transformation_job_repository

    def handle(self, command: AddTransformationJobCommand):
        transform = TransformationJob(id=command.id,
                                      model=command.model,
                                      process_config=command.process_config,
                                      active=command.active,
                                      status=command.status,
                                      is_dependent=command.is_dependent,
                                      dependent_jobs=command.dependent_jobs,
                                      frequency=command.frequency,
                                      created_on=utc_now(),
                                      updated_on=utc_now(),
                                      created_by=command.created_by,
                                      updated_by=command.updated_by,
                                      last_success_date=utc_now(),
                                      job_type=command.job_type,
                                      retry_count=command.retry_count,
                                      last_execution_detail=command.last_execution_detail
                                      )
        self.transformation_job_repository.add(transform)


