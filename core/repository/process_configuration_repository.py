from core.entities.process_configuration import ProcessConfiguration


class ProcessConfigurationRepository:

    def add(self, process_config: ProcessConfiguration):
        process_config.save()

    def get(self, name):
        process_config = ProcessConfiguration.get(name)
        return process_config
