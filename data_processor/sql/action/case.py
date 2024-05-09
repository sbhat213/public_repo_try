from data_processor.sql.action.action import Action
from data_processor.sql.constant import Map
from data_processor.utils.step_reader import get_case_conditions
from data_processor.sql.constant import Condition


class Case(Action):

    def __init__(self):
        super().__init__()

    def map(self, k, v, left_table, right_table):
        name_val = self.get_name_val(k, left_table)
        conditions = get_case_conditions(v)
        case_statement = "(case"
        for condition in conditions:
            field = condition[Condition.FIELD]
            then = condition[Condition.THEN]
            when = f"{right_table}.{field}"
            value = condition[Condition.IS]
            # Checking for else condition if present then add else condition
            if Condition.ELSE in condition:
                if condition[Condition.ELSE] == Condition.COLUMN:
                    el = f" else {right_table}.{field}"
                    when_condition = f"  when {when} ='{value}' then '{then}' {el}"
                elif condition[Condition.ELSE] == Condition.BLANK:
                    el = f" else '' "
                    when_condition = f"  when {when} ='{value}' then '{then}' {el}"
                elif condition[Condition.ELSE] != Condition.COLUMN or condition[Condition.ELSE] != Condition.BLANK:
                    default = condition[Condition.ELSE]
                    el = f" else '{default}' "
                    when_condition = f"  when {when} ='{value}' then '{then}' {el}"
                else:
                    when_condition = f"  when {when} ='{value}' then '{then}' "
            else:
                when_condition = f"  when {when} ='{value}' then '{then}' "

            case_statement += when_condition

        case_statement += " end)"
        return name_val, case_statement
