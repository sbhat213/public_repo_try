import boto3
from math import ceil
from settings import Settings


def shuffle_partition(self, input_path):
    if input_path != '':
        print("input_path", input_path)
        key = input_path.split('//')[1]
        bucket_nm = key.split('/', 1)[0]
        file_nm = key.split('/', 1)[1]

        print("key : ", key)
        print("bucket_nm : ", bucket_nm)
        print("file_nm : ", file_nm)

        client = boto3.client(service_name='s3', use_ssl=True)

        response = client.head_object(
            Bucket=bucket_nm,
            Key=file_nm
        )
        size = response['ContentLength']
        print('File size in KB : ', size)

        if size == 0:
            partition = 1
        else:
            partition_size = Settings.SHUFFLE_PARTITION["partition_size"]
            size_in_mb = ceil(size / (1000 ** 2))
            partition = ceil(size_in_mb / partition_size)
            print('File size in MB : ', size_in_mb, ', Partition size used : ', partition_size)

    else:
        transformation_partition = Settings.SHUFFLE_PARTITION["transformation_partition"]
        partition = transformation_partition

    return partition
