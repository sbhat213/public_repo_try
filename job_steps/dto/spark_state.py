from types import SimpleNamespace
from job_steps.dto.task import Task


class SparkState:
    def __init__(self):
        self.task = {}
        self.livyUrl = ""
        self.response_id = ""

    def set_values(self, data):
        obj = SimpleNamespace(**data)
        self.task = Task(**obj.task)
        self.livyUrl = obj.livyUrl
        self.response_id = obj.response_id
