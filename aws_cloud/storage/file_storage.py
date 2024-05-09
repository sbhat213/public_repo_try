import abc
from abc import ABCMeta


class FileStorage(metaclass=ABCMeta):
    def __init__(self):
        super(FileStorage, self).__init__()

    @abc.abstractmethod
    def create_presigned_url(self, bucket_name, object_name, expiration=3600,
                             metadata=None, request_type="put_object"):
        pass

    @abc.abstractmethod
    def get_object_head(self, bucket_name, key):
        pass

    @abc.abstractmethod
    def list_files(self, bucket_name, prefix):
        pass

    @abc.abstractmethod
    def move_files(self, bucket_name, prefix, destination):
        pass

    @abc.abstractmethod
    def delete_files(self, bucket_name, prefix, skip):
        pass

    @abc.abstractmethod
    def list_object(self, bucket_name, prefix):
        pass

    @abc.abstractmethod
    def get_object_head(self, bucket_name, key):
        pass

    def get_object(self, bucket_name, key):
        pass

    @abc.abstractmethod
    def get_object(self, bucket_name, key):
        pass

    @abc.abstractmethod
    def put_object(self, key_name, bucket_name, contents, content_type):
        pass

    @abc.abstractmethod
    def upload_file(self, file_name, bucket, object_name=None):
        pass
