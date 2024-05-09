import os
from utilities.config_uploader import injector, model_dir_path
import json
from core.command.add_model_command import AddModelCommand
from core.command.handler.add_model_handler import AddModelHandler


class ModelUploader:

    def config_processor(self):

        handler = injector.get(AddModelHandler)
        processing_dir = os.listdir(model_dir_path)

        # Iterating over files present in directory
        for file in processing_dir:
            file_name = model_dir_path + "\\" + file

            if file_name.endswith(".json"):
                print("processed model fileName :" + file_name)
                f = open(file_name)
                data = json.load(f)
                name = data['name']
                path = data['path']
                p_key = data['primary_key']
                c_key = data['composite_key']
                partition_key = data['partition_key']
                table = data['table_name']
                field = data['fields']

                # parameters (name, path, field, p_key, partition_key, table, created_by, updated_by)
                command = AddModelCommand(name, path, field, p_key, c_key, partition_key, table,
                                          "config_uploader", "config_uploader")
                handler.handle(command)
            else:
                print("invalid model fileName :" + file_name)
