import abc
from abc import ABCMeta
from pynamodb.models import Model


class NoSQLDB(metaclass=ABCMeta):
    def __init__(self):
        super(NoSQLDB, self).__init__()

    @abc.abstractmethod
    def create_table(self, model, billing_mode, wait=True):
        pass

    @abc.abstractmethod
    def delete_table(self, table_name):
        pass

    @abc.abstractmethod
    def truncate_table(self, model: Model):
        pass
