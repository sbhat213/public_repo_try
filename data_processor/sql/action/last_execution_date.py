from data_processor.sql.action.action import Action
from data_processor.sql.constant import Map


class LastExecutionDate(Action):

    def __init__(self):
        super().__init__()

    def map(self, k, v, left_table, right_table, last_execution_date):
        name_val = self.get_name_val(k, left_table)
        if k == "_":
            field = v[Map.FIELD]
            name_val = f"{right_table}.{field}"
        value = f"'{last_execution_date}'"
        return name_val, value
