class UpdateFileStatusCommand:
    def __init__(self, file_id: str, status: str, updated_by: str, updated_on: str, last_success_date: str,
                 execution_details = None):
        self.file_id = file_id
        self.status = status
        self.updated_by = updated_by
        self.updated_on = updated_on
        self.last_success_date = last_success_date
        self.execution_details = execution_details

