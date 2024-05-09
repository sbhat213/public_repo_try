from data_processor.sql.action.action import Action
from data_processor.sql.constant import Map

class Fixed(Action):

    def __init__(self):
        super().__init__()

    def map(self, k, v, left_table, right_table):
        name_val = self.get_name_val(k, left_table)
        if k == "_":
            field = v[Map.FIELD]
            name_val = f"{right_table}.{field}"
        fixed_value = v[Map.VALUE]
        value = f"'{fixed_value}'"
        return name_val, value
