from aws_cloud.function.aws_lambda import AWSLambda
from aws_cloud.function.severless import Serverless
from aws_cloud.storage.file_storage import FileStorage
from aws_cloud.storage.s3 import AWSStorage
from aws_deployment import *
from aws_deployment.common_configuration import CommonConfiguration
from injector import Injector, singleton

from settings import Settings

injector = Injector(modules=[CommonConfiguration])
injector.binder.bind(FileStorage, to=AWSStorage(settings=Settings.AWS_CREDENTIALS), scope=singleton)
injector.binder.bind(Serverless, to=AWSLambda(settings=Settings.AWS_CREDENTIALS), scope=singleton)

