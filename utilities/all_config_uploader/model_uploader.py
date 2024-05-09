import os
from utilities.all_config_uploader import injector, model_dir_path
import json
from core.command.add_model_command import AddModelCommand
from core.command.handler.add_model_handler import AddModelHandler


class ModelUploader:

    def config_processor(self):

        handler = injector.get(AddModelHandler)
        processing_dir = os.listdir(model_dir_path)
        print(processing_dir)

        # Iterating over folder present in model directory
        for folder in processing_dir:
            if folder != '__init__.py':
                folder_ = model_dir_path + "\\" + folder
                folder_nm = os.listdir(folder_)
                print("processed model Folder : " + str(folder))

                for file in folder_nm:
                    file_name = folder_ + "\\" + file
                    # print("processed model fileName :" + file)

                    if file_name.endswith(".json"):
                        print("processed model fileName : " + file_name)
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
                        print("invalid model fileName : " + file_name)
