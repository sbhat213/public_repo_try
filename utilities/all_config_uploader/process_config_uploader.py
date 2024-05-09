import os
from utilities.all_config_uploader import injector, process_config_dir_path
import json
from core.command.add_process_configuration_command import AddProcessConfigurationCommand
from core.command.handler.add_process_configuration_handler import AddProcessHandler


class ProcessUploader:

    def config_processor(self):

        handler = injector.get(AddProcessHandler)
        processing_dir = os.listdir(process_config_dir_path)
        print(processing_dir)

        # Iterating over folders present in process_config directory
        for folder in processing_dir:
            if folder != '__init__.py':
                folder_ = process_config_dir_path + "\\" + folder
                folder_nm = os.listdir(folder_)
                print("processed process_config Folder : " + str(folder))
                for file in folder_nm:
                    file_name = folder_ + "\\" + file
                    # print("processed process_config Folder :" + file)

                    if file_name.endswith(".json"):
                        print("processed process_config file Name : " + file_name)
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
                        print("invalid process_config file Name : " + file_name)
