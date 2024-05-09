from data_processor.sql.constant import Map


class Action:

    def get_name_val(self, k, left_table):
        return f"{left_table}.{k}"
