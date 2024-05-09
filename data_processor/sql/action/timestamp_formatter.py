from data_processor.sql.action.action import Action
from data_processor.sql.constant import Map


class TimestampFormatter(Action):

    def __init__(self):
        super().__init__()

    def map(self, k, v, left_table, right_table):
        name_val = self.get_name_val(k, left_table)
        field = v[Map.FIELD]
        value = f"coalesce(to_timestamp({right_table}.{field}, 'yyyy-MM-dd HH:mm:ss'), " \
                f"to_timestamp({right_table}.{field}, 'dd-MM-yyyy HH:mm:ss'), " \
                f"to_timestamp({right_table}.{field}, 'MM-dd-yyyy HH:mm:ss'))"

        return name_val, value
