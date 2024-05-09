from datetime import datetime
from shared.utils.datetime import utc_now
from injector import inject, singleton
from core.command.add_process_configuration_command import AddProcessConfigurationCommand
from core.entities.process_configuration import ProcessConfiguration
from core.repository.process_configuration_repository import ProcessConfigurationRepository


@singleton
class AddProcessHandler:
    @inject
    def __init__(self, process_configuration_repository: ProcessConfigurationRepository):
        self.process_configuration_repository = process_configuration_repository

    def handle(self, command: AddProcessConfigurationCommand):
        process = ProcessConfiguration(name=command.name,
                                       steps=command.steps,
                                       depends_on=command.depends_on,
                                       global_map=command.global_map,
                                       created_on=utc_now(),
                                       updated_on=utc_now()
                                       )
        self.process_configuration_repository.add(process)
