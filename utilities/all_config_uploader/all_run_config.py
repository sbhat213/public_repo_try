from utilities.all_config_uploader.file_model_uploader import FileModelUploader
from utilities.all_config_uploader.model_uploader import ModelUploader
from utilities.all_config_uploader.process_config_uploader import ProcessUploader
from utilities.all_config_uploader.transformation_uploader import TransformationUploader

# Pass config_file folder name in run_file list for which Dynamodb update required.

# run_file = ['Model', 'ProcessConfig', 'FileModel', 'TransformationJob']

run_file = ['Model', 'ProcessConfig', 'FileModel', 'TransformationJob']

for file in run_file:
    if file == 'FileModel':
        t = FileModelUploader()
        t.config_processor()
        print('++++++++Uploaded FileModel Successfully++++++++')

    elif file == 'Model':
        t = ModelUploader()
        t.config_processor()
        print('++++++++Uploaded Model Successfully++++++++')

    elif file == 'ProcessConfig':
        t = ProcessUploader()
        t.config_processor()
        print('++++++++Uploaded ProcessConfig Successfully++++++++')

    elif file == 'TransformationJob':
        t = TransformationUploader()
        t.config_processor()
        print('++++++++Uploaded TransformationJob Successfully++++++++')

    else:
        print('Invalid folder name mentioned in run_file list:' + file)


