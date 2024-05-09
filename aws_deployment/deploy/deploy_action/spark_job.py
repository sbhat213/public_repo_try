import os
from os import path

from injector import inject

from aws_cloud.storage.file_storage import FileStorage
from shared.utils.zip import ZIP

from aws_deployment.util.directory_util import DirectoryUtil


class SparkJob:

    @inject
    def __init__(self, storage: FileStorage):
        self.storage = storage

    def deploy(self, inputs, config):
        # set directory structure
        DirectoryUtil.set_directory(config)

        # variables
        step_name = config["name"]
        bucket = config["aws"]["bucket"]
        dir = config["aws"]["dir"]
        processor_file = config["processor_file"]

        deploy_path = config["deploy_path"]

        # zip the bundle
        zip_file_name = step_name + ".zip"
        zip_file_path = os.path.join(deploy_path, zip_file_name)
        zip_file_name = os.path.join(dir, zip_file_name)
        ZIP.make_zip(deploy_path, zip_file_path)

        # Upload to s3 zip bundle
        self.storage.upload_file(zip_file_path, bucket, zip_file_name)
        processor_file_path = os.path.join(deploy_path, processor_file)
        # Upload to s3 processor file
        self.storage.upload_file(processor_file_path, bucket, os.path.join(dir, path.basename(processor_file_path)))
