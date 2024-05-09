from injector import Module, singleton

from aws_deployment.deploy.deploy_action.python_environment import PythonEnvironment
from aws_deployment.deploy.deploy_action.spark_job import SparkJob
from aws_deployment.deploy.deploy_action.function import Lambda
from settings import Settings


class CommonConfiguration(Module):

    def configure(self, binder):
        # binder.bind(FileStorage, to=AWSStorage(settings=Settings.AWS_CREDENTIALS), scope=singleton)
        binder.bind(Settings, to=Settings, scope=singleton)

        binder.bind('Lambda', to=Lambda, scope=singleton)
        binder.bind('PythonEnvironment', to=PythonEnvironment, scope=singleton)
        binder.bind('SparkJob', to=SparkJob, scope=singleton)


