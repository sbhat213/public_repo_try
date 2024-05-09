import os
import subprocess

from injector import inject

from aws_deployment.util.directory_util import DirectoryUtil
from shared.utils.zip import ZIP

from aws_cloud.storage.file_storage import FileStorage

class PythonEnvironment:

    @inject
    def __init__(self, storage: FileStorage):
        self.storage = storage

    def deploy(self, inputs, config):
        # set directory structure
        DirectoryUtil.set_directory(config, False)

        # variables
        step_name = config["name"]
        bucket = config["aws"]["bucket"]
        dir = config["aws"]["dir"]

        deploy_path = config["deploy_path"]
        deploy_app_path = config["deploy_app_path"]

        source_path = config["source_path"]
        source_app_path = config["source_app_path"]

        # create environment
        requirements_path = os.path.join(source_path, source_app_path, config['requirements_path'])
        subprocess.call(" pip install -r " + requirements_path + " -t " + os.path.join(deploy_path, deploy_app_path),
                        shell=True)

        # zip the bundle
        zip_file_name = step_name + ".zip"
        zip_file_path = os.path.join(deploy_path, zip_file_name)
        zip_file_name = os.path.join(dir, zip_file_name)
        ZIP.make_zip(deploy_path, zip_file_path)


        # Upload to s3
        self.storage.upload_file(zip_file_path, bucket, zip_file_name)
