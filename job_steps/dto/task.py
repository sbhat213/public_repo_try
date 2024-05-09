class Task:

    def __init__(self, model, task_type, process_config, task_id=None, url="",job_type =None,last_success_date = None,stage_path =None):
        self.task_type = task_type
        self.process_config = process_config
        self.model = model
        self.task_id = task_id
        self.url = url
        self.job_type = job_type
        self.last_success_date =  last_success_date
        self.stage_path = stage_path



