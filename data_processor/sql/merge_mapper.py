from datetime import timedelta

from shared.utils.datetime import utc_now
from data_processor.sql.constant import Map, Action, Comparison, Operator
from data_processor.sql.action import injector


class MergeMapper:
    def __init__(self):
        self.last_execution_date = None

    def get_sql_join_update_map(self, step_map, left_table, right_table):
        names = []
        values = []
        self.map_join_insert_fields(names, values, step_map, left_table, right_table, True)
        fields = []
        for k, v in zip(names, values):
            param = "{}={}".format(k, v)
            fields.append(param)
        return " ,".join(fields)

    def get_sql_join_insert_map(self, step_map, left_table, right_table):
        names = []
        values = []
        self.map_join_insert_fields(names, values, step_map, left_table, right_table)
        names_query = " ,".join(names)
        values_query = " ,".join(values)
        return f"({names_query}) values ({values_query})"

    def get_sql_join_filter_map(self, step_map, left_table, right_table):
        names = []
        values = []
        and_fields = []
        or_fields = []
        self.__map_field_filter_action(names, values, step_map, left_table, right_table)
        for k, v in zip(names, values):
            key = k.split(".")[1]
            if key not in step_map:
                key = "_"
            item = step_map[key]
            if Map.COMPARISON in item:
                if item[Map.COMPARISON] == Comparison.NOT:
                    param = f"{k}!={v}"
                elif item[Map.COMPARISON] == Comparison.NOT_OR_NULL:
                    param = f"(isnull({k}) or {k}!={v})"
                elif item[Map.COMPARISON] == Comparison.LESS:
                    param = f"{k}<{v}"
                elif item[Map.COMPARISON] == Comparison.GREATER:
                    param = f"{k}>{v}"
            else:
                param = f"{k}={v}"
            if Map.OPERATOR in item:
                if item[Map.OPERATOR] == Operator.OR:
                    or_fields.append(param)
                else:
                    and_fields.append(param)
            else:
                and_fields.append(param)
        and_condition = " and ".join(and_fields)
        or_condition = " or ".join(or_fields)
        if len(and_condition) == 0:
            return or_condition
        elif len(or_condition) == 0:
            return and_condition
        else:
            return f"{and_condition} or {or_condition}"

    def __map_field_filter_action(self, name, values, items, left_table, right_table):
        for k, v in items.items():
            has_action = Map.ACTION in v
            has_field = Map.FIELD in v
            if has_action:
                action = v[Map.ACTION]
                # if action == Action.FIXED:
                self.__map_field_action(k, v, name, values, left_table, right_table)
                # fixed_value = v[Map.VALUE]
                # values.append(f"'{fixed_value}'")
            elif has_field:
                field = v[Map.FIELD]
                name.append(f"{left_table}.{k}")
                values.append(f"{right_table}.{field}")

    def map_join_insert_fields(self, name, values, items, left_table, right_table, is_update=False):
        for k, v in items.items():
            has_action = Map.ACTION in v
            has_field = Map.FIELD in v

            if is_update and "set_on_update" in v:
                continue

            if has_action:
                self.__map_field_action(k, v, name, values, left_table, right_table)
            elif has_field:
                field = v[Map.FIELD]
                name.append(f"{left_table}.{k}")
                values.append(f"{right_table}.{field}")

    def __map_field_action(self, k, v, name, values, left_table, right_table):
        action = v[Map.ACTION]
        if action == Action.LAST_EXECUTION_DATE:
            name_val, value = injector.get(action).map(k, v, left_table, right_table, self.last_execution_date)
        else:
            name_val, value = injector.get(action).map(k, v, left_table, right_table)

        name.append(name_val)
        values.append(value)
