from data_processor.utils.step_reader import *


class BaseJoinMapper:

    def __init__(self, mapper):
        self.mapper = mapper
        self.table_1_alias = "t1"
        self.table_2_alias = "t2"

    def get_filter_query(self, filter_map):
        return self.mapper.get_sql_join_filter_map(filter_map, self.table_1_alias, self.table_2_alias)

    def get_update_query(self, matched_map, field_map):
        update_query = self.mapper.get_sql_join_update_map(field_map,
                                                           self.table_1_alias, self.table_2_alias
                                                           )

        if len(matched_map) > 0:
            match_condition = self.mapper.get_sql_join_filter_map(matched_map, self.table_1_alias, self.table_2_alias)
            return f"when  matched and {match_condition} then update set {update_query}"

        return f"when  matched  then update set {update_query}"

    def get_insert_query(self, field_map):
        insert_query = self.mapper.get_sql_join_insert_map(field_map,
                                                           self.table_1_alias, self.table_2_alias)
        return f"when not matched then insert {insert_query}"
