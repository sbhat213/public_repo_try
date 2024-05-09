import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from aws_cloud.storage.file_storage import FileStorage


class AWSStorage(FileStorage):
    def __init__(self, settings):
        if settings is None:
            self.client = boto3.client(
                "s3"
            )
        else:
            self.client = boto3.client(
                "s3",
                aws_access_key_id=settings["access_key_id"],
                aws_secret_access_key=settings["secret_access_key"],
                region_name=settings["region"],
                config=Config(signature_version='s3v4', s3={"use_accelerate_endpoint": False})
            )

    def create_presigned_url(self, bucket_name, object_name, expiration=3600,
                             metadata=None, request_type="put_object"):

        try:
            params = {'Bucket': bucket_name,
                      'Key': object_name}
            if metadata:
                params['Metadata'] = metadata
            response = self.client.generate_presigned_url(request_type,
                                                          Params=params,
                                                          ExpiresIn=expiration)
        except ClientError as e:
            return None

        # The response contains the presigned URL
        return response

    def get_object_head(self, bucket_name, key):
        object_summary = self.client.head_object(
            Bucket=bucket_name,
            Key=key
        )
        return object_summary

    def move_files(self, bucket_name, source_prefix, destination_prefix, keys, skip):
        for item in keys:
            key = item["Key"]
            if key.split(".")[-1] in skip:
                copy_source = {
                    'Bucket': bucket_name,
                    'Key': key
                }
                destination_file = key.replace(source_prefix, "")
                self.client.copy(copy_source, bucket_name, destination_prefix + destination_file)
                self.client.delete_object(Bucket=bucket_name, Key=key)

    def delete_files(self, bucket_name, keys, skip):
        for item in keys:
            key = item["Key"]
            if key.split(".")[-1] not in skip:
                self.client.delete_object(Bucket=bucket_name, Key=key)

    def list_files(self, bucket_name, prefix):
        result = []
        response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" in response:
            result = response["Contents"]
        return result

    def list_object(self, bucket_name, prefix, delimiter='/'):
        result = []
        response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter)
        if "CommonPrefixes" in response:
            result = response["CommonPrefixes"]
        return result

    def get_object(self, bucket_name, key):
        file = self.client.get_object(
            Bucket=bucket_name,
            Key=key
        )
        return file

    def put_object(self, key_name, bucket_name, contents, content_type):
        try:
            self.client.put_object(Key=key_name, Bucket=bucket_name, Body=contents,
                                   ContentType=content_type)
        except ClientError as e:
            print(e)
            return False
        return True

    def upload_file(self, file_name, bucket, object_name=None):
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        try:
            self.client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            print(e)
            return False
        return True
