from datetime import datetime
from shared.utils.datetime import utc_now
from injector import inject, singleton
from core.command.add_file_model_command import AddFileModelCommand
from core.entities.file_model import FileModel
from core.repository.file_model_repository import FileModelRepository


@singleton
class AddFileModelHandler:
    @inject
    def __init__(self, file_model_repository : FileModelRepository):
        self.file_model_repository = file_model_repository

    def handle(self, command: AddFileModelCommand):
        process = FileModel(file=command.file,
                            model=command.model,
                            created_on=utc_now(),
                            updated_on=utc_now()
                            )
        self.file_model_repository.add(process)
