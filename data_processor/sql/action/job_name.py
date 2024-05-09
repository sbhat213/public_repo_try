from data_processor.sql.action.action import Action
from data_processor.sql.constant import Map


class JobName(Action):

    def __init__(self):
        super().__init__()

    def map(self, k, v, left_table, right_table):
        name_val = self.get_name_val(k, left_table)
        value = f"'spark_job'"
        return name_val, value
