from types import SimpleNamespace
from job_steps.dto.task import Task
from job_steps.dto.emr_cluster import Emr


class ScheduleState:

    def __init__(self):
        self.tasks = []
        self.emr_cluster = {}
        self.create_cluster = False
        self.tasks_in_progress = False
        self.check_emr_cluster_choice = ""
        self.available_slots = 0
        self.wait_cluster_creation = 0

    def set_values(self, data):
        obj = SimpleNamespace(**data)
        self.tasks = obj.tasks
        self.emr_cluster = obj.emr_cluster
        self.create_cluster = obj.create_cluster
        self.tasks_in_progress = obj.tasks_in_progress
        self.check_emr_cluster_choice = obj.check_emr_cluster_choice
        self.wait_cluster_creation = obj.wait_cluster_creation
        # for item in obj.tasks:
        #     task = Task(**item)
        #     # task.task_type = tsk.task_type
        #     # task.process_config_name = tsk.process_config_name
        #     # task.model = tsk.model
        #     # task.file_id = tsk.file_id
        #     self.tasks.append(task)
