from types import SimpleNamespace
from job_steps.dto.check_spark_state import CheckSparkState


class StatusScheduleState:

    def __init__(self):
        self.css_tasks = []
        self.available_slots = 0

    def set_values(self, data):
        obj = SimpleNamespace(**data)
        self.css_tasks = obj.css_tasks
        self.available_slots = obj.available_slots
