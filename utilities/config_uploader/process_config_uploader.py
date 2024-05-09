import os
from utilities.config_uploader import injector, process_config_dir_path
import json
from core.command.add_process_configuration_command import AddProcessConfigurationCommand
from core.command.handler.add_process_configuration_handler import AddProcessHandler


class ProcessUploader:

    def config_processor(self):

        handler = injector.get(AddProcessHandler)
        processing_dir = os.listdir(process_config_dir_path)

        # Iterating over files present in directory
        for file in processing_dir:
            file_name = process_config_dir_path + "\\" + file

            if file_name.endswith(".json"):
                print("processed model file Name :" + file_name)
                f = open(file_name)
                data = json.load(f)
                name = data['name']
                steps = data['steps']
                global_map = data['global_map']
                depends_on = data['depends_on']

                # parameters (name, steps, global_map, depends_on, created_by, updated_by)
                command = AddProcessConfigurationCommand(name, steps, depends_on, global_map)
                handler.handle(command)
            else:
                print("invalid model file Name :" + file_name)
