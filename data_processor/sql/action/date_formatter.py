from data_processor.sql.action.action import Action
from data_processor.sql.constant import Map


class DateFormatter(Action):

    def __init__(self):
        super().__init__()

    def map(self, k, v, left_table, right_table):
        name_val = self.get_name_val(k, left_table)
        field = v[Map.FIELD]
        value = f"coalesce(to_date({right_table}.{field}, 'yyyy-MM-dd'), " \
                f"to_date({right_table}.{field}, 'dd-MM-yyyy'), " \
                f"to_date({right_table}.{field}, 'MM-dd-yyyy'), " \
                f"to_date({right_table}.{field}, 'yyyy/MM/dd'), " \
                f"to_date({right_table}.{field}, 'dd/MM/yyyy'), " \
                f"to_date({right_table}.{field}, 'MM/dd/yyyy'))"

        return name_val, value
