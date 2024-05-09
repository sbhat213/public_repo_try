from types import SimpleNamespace


class CheckSparkState:
    def __init__(self, model="", response_id="", livy_url="",
                 task_id="", task_type="", job_type="",
                 updated_on="", status="", error_code="",
                 error="", application_id=""):
        self.model = model
        self.response_id = response_id
        self.livy_url = livy_url
        self.task_id = task_id
        self.task_type = task_type
        self.job_type = job_type
        self.updated_on = updated_on
        self.status = status
        self.error_code = error_code
        self.error = error
        self.application_id = application_id

    def set_values(self, data):
        obj = SimpleNamespace(**data)
        self.model = obj.model
        self.response_id = obj.response_id
        self.livy_url = obj.livy_url
        self.task_id = obj.task_id
        self.task_type = obj.task_type
        self.job_type = obj.job_type
        self.updated_on = obj.updated_on
        self.status = obj.status
        self.error_code = obj.error_code
        self.error = obj.error
        self.application_id = obj.application_id
