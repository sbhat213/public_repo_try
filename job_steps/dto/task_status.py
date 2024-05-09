class TaskStatus:

    def __init__(self,response_id,error,error_code,livy_url):
        self.response_id = response_id
        self.error = error
        self.error_code = error_code
        self.livy_url = livy_url



