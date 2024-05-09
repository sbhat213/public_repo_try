import abc
from abc import ABCMeta

from aws_cloud.function.function_detail import FunctionDetail


class Serverless(metaclass=ABCMeta):
    def __init__(self):
        super(Serverless, self).__init__()

    @abc.abstractmethod
    def update_function_code(self, detail: FunctionDetail):
        pass
