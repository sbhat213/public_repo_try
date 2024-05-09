import os
from utilities.all_config_uploader import injector, transform_dir_path
import json
from core.command.add_transformation_job_command import AddTransformationJobCommand
from core.command.handler.add_transformation_job_handler import AddTransformationHandler


class TransformationUploader:

    def config_processor(self):

        handler = injector.get(AddTransformationHandler)
        processing_dir = os.listdir(transform_dir_path)
        print(processing_dir)

        # Iterating over folder present in transformation directory
        for folder in processing_dir:
            if folder != '__init__.py':
                folder_ = transform_dir_path + "\\" + folder
                folder_nm = os.listdir(folder_)
                print("processed transformation Folder : " + str(folder))

                for file in folder_nm:
                    file_name = folder_ + "\\" + file
                    # print("processed transformation Folder :" + file)

                    if file_name.endswith(".json"):
                        print("processed transformation fileName : " + file_name)
                        f = open(file_name)
                        data = json.load(f)
                        id = data['id']
                        model = data['model']
                        process_config = data['process_config']
                        active = data['active']
                        status = data['status']
                        is_dependent = data['is_dependent']
                        dependent_jobs = data['dependent_jobs']
                        frequency = data['frequency']
                        job_type = data['job_type']
                        retry_count = data['retry_count']
                        last_execution_detail = data['last_execution_detail']

                        # print(id)
                        # print(model)
                        # print(process_config)
                        # print(active)
                        # print(status)
                        # print(frequency)
                        # parameters (id, model, process_config, active, status, frequency)
                        command = AddTransformationJobCommand(id, model, process_config, active,
                                                              status, is_dependent, dependent_jobs,
                                                              frequency, "config_uploader", "",
                                                              job_type, retry_count, last_execution_detail
                                                              )
                        handler.handle(command)
                    else:
                        print("invalid transformation fileName : " + file_name)
