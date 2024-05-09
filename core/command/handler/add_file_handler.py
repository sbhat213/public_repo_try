from datetime import datetime
from shared.utils.datetime import utc_now, year_month

from bson import ObjectId
from injector import inject, singleton

from core.command.add_file_command import AddFileCommand
from core.entities.file import File
from core.entities.file_status import FileStatus
from core.repository.file_repository import FileRepository
from core.repository.file_model_repository import FileModelRepository
import re


@singleton
class AddFileHandler:
    @inject
    def __init__(self, file_repository: FileRepository, file_model_repository: FileModelRepository):
        self.file_repository = file_repository
        self.file_model_repository = file_model_repository

    def handle(self, command: AddFileCommand):
        file_type = re.sub('_(\\d+)', "", command.name)
        identifier = (file_type.split("."))[0]
        file_model = self.file_model_repository.get(identifier.lower())
        file = File(id=str(ObjectId()),
                    name=command.name,
                    status=FileStatus.File_Submitted.value,
                    url=command.url,
                    model=file_model.model,
                    created_on=utc_now(),
                    updated_on=utc_now(),
                    created_by=command.created_by,
                    updated_by=command.created_by,
                    created_month=year_month(),
                    retry_count=0
                    )
        self.file_repository.add(file)
