from data_processor.sql.action.action import Action
from data_processor.sql.constant import Map


class Concat(Action):

    def __init__(self):
        super().__init__()

    def map(self, k, v, left_table, right_table):
        name_val = self.get_name_val(k, left_table)
        field = v[Map.FIELD]
        concat = f", {right_table}.".join([''] + field)
        trim_concat = concat[1:]
        value = f"concat_ws(' ', {trim_concat})"
        return name_val, value
