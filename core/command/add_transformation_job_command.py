class AddTransformationJobCommand:
    def __init__(self, id: str, model, process_config, active, status, is_dependent, dependent_jobs,
                 frequency, created_by, updated_by, job_type, retry_count, last_execution_detail ):

        self.id = id
        self.model = model
        self.process_config = process_config
        self.active = active
        self.status = status
        self.is_dependent = is_dependent
        self.dependent_jobs = dependent_jobs
        self.frequency = frequency
        self.created_by = created_by
        self.updated_by = updated_by
        self.job_type = job_type
        self.retry_count = retry_count
        self.last_execution_detail = last_execution_detail

