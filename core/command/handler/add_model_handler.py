from datetime import datetime
from shared.utils.datetime import utc_now
from injector import inject, singleton
from core.command.add_model_command import AddModelCommand
from core.entities.model import Model
from core.repository.model_repository import ModelRepository


@singleton
class AddModelHandler:
    @inject
    def __init__(self, model_repository: ModelRepository):
        self.model_repository = model_repository

    def handle(self, command: AddModelCommand):
        model = Model(name=command.name,
                      path=command.path,
                      fields=command.fields,
                      primary_key=command.primary_key,
                      composite_key=command.composite_key,
                      partition_key=command.partition_key,
                      table_name=command.table_name,
                      created_on=utc_now(),
                      updated_on=utc_now(),
                      created_by=command.created_by,
                      updated_by=command.created_by
                      )
        self.model_repository.add(model)
