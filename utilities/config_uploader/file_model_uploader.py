import os
from utilities.config_uploader import injector, file_model_dir_path
import json
from core.command.add_file_model_command import AddFileModelCommand
from core.command.handler.add_file_model_handler import AddFileModelHandler


class FileModelUploader:

    def config_processor(self):

        handler = injector.get(AddFileModelHandler)
        processing_dir = os.listdir(file_model_dir_path)

        # Iterating over files present in directory
        for file in processing_dir:
            file_name = file_model_dir_path + "\\" + file

            if file_name.endswith(".json"):
                print("processed FileModel file Name :" + file_name)
                f = open(file_name)
                data = json.load(f)
                for i, j in data.items():
                    # print(i + j)
                    file1 = i
                    model = j

                    # parameters (file, model)
                    command = AddFileModelCommand(file1, model)
                    handler.handle(command)
            else:
                print("invalid FileModel file Name :" + file_name)
