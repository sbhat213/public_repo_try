from data_processor.sql.action.action import Action
from data_processor.sql.constant import Map
from shared.utils.datetime import utc_now_string


class CurrentDate(Action):

    def __init__(self):
        super().__init__()

    def map(self, k, v, left_table, right_table):
        name_val = self.get_name_val(k, left_table)
        value = f"'{utc_now_string()}'"
        return name_val, value
