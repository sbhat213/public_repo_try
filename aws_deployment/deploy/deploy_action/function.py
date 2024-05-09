import os

from injector import inject

from aws_cloud.function.function_detail import FunctionDetail
from aws_cloud.function.severless import Serverless
from aws_cloud.storage.file_storage import FileStorage
from aws_deployment.util.directory_util import DirectoryUtil
from shared.utils.zip import ZIP


class Lambda:

    @inject
    def __init__(self, storage: FileStorage, function: Serverless):
        self.storage = storage
        self.function = function

    def deploy(self, inputs, config):
        # set directory structure
        DirectoryUtil.set_directory(config)

        # variables
        step_name = config["name"]
        function_name = config["aws"]["function_name"]
        bucket = config["aws"]["bucket"]
        dir = config["aws"]["dir"]

        deploy_path = config["deploy_path"]

        # zip the bundle
        zip_file_name = step_name + ".zip"
        zip_file_path = os.path.join(deploy_path, zip_file_name)
        zip_file_name = os.path.join(dir, zip_file_name)
        ZIP.make_zip(deploy_path, zip_file_path)

        # Upload to s3
        self.storage.upload_file(zip_file_path, bucket, zip_file_name)

        # Update function with latest build
        fd = FunctionDetail(function_name, bucket, zip_file_name)
        self.function.update_function_code(fd)
