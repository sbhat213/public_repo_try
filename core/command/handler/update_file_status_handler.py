from injector import inject, singleton
from core.command.update_file_status_command import UpdateFileStatusCommand
from core.repository.file_repository import FileRepository
from core.entities.file_status import FileStatus


@singleton
class UpdateFileStatusCommandHandler:
    @inject
    def __init__(self, file_repository: FileRepository):
        self.file_repository = file_repository

    def handle(self, command: UpdateFileStatusCommand):
        file = self.file_repository.get(command.file_id)
        file.status = command.status
        file.updated_by = command.updated_by
        file.updated_on = command.updated_on
        if command.status == FileStatus.Processed.value:
            file.retry_count = 0
            file.last_success_date = command.last_success_date

        file.file_execution_details = command.execution_details

        if command.status == FileStatus.Failed.value:
            file.retry_count = file.retry_count + 1
        self.file_repository.add(file)
