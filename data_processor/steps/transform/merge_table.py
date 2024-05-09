from data_processor.utils.step_reader import *
from data_processor.sql.merge_mapper import MergeMapper
from injector import inject
from data_processor.sql.join.self_join_mapper import SelfJoinMapper
from data_processor.sql.join.single_join_mapper import SingleJoinMapper
from data_processor.sql.join.multi_join_mapper import MultiJoinMapper
from settings import Settings


class MergeTable:

    @inject
    def __init__(self, mapper: MergeMapper, self_join_mapper: SelfJoinMapper,
                 single_join_mapper: SingleJoinMapper, multi_join_mapper: MultiJoinMapper):
        self.mapper = mapper
        self.self_join_mapper = self_join_mapper
        self.single_join_mapper = single_join_mapper
        self.multi_join_mapper = multi_join_mapper

    def execute(self, params):
        join = get_join_type(params.step_config)
        print("Join Type : ", join)
        if join == "self":
            insert_query, update_query, filter_query, table_2, table_1 = self.self_join_mapper.join(params)
        elif join == "multi":
            insert_query, update_query, filter_query, table_2, table_1 = self.multi_join_mapper.join(params)
        else:
            insert_query, update_query, filter_query, table_2, table_1 = self.single_join_mapper.join(params)

        # Added for testing
        if Settings.LOG_FLAG['print_show_count']:
            table_1_sql = params.spark.sql(f"SELECT * from {table_1}")
            print("Before merge table_1 : ", table_1, ", Count : ", table_1_sql.count())
            table_1_sql.show(5, False)
            table_2_sql = params.spark.sql(f"SELECT * from {table_2}")
            print("Before merge table_2 : ", table_2, ", Count : ", table_2_sql.count())
            table_2_sql.show(5, False)

        query = f"Merge into {table_1} as t1 using {table_2} as t2  \
                on {filter_query} \
                {update_query} \
                {insert_query}"
        print(query)
        delta_table = params.spark.sql(query)
        # delta_table.show()

        # Added for testing
        if Settings.LOG_FLAG['print_show_count']:
            table_1_sql = params.spark.sql(f"SELECT * from {table_1}")
            print("After merge table_1 : ", table_1, ", Count : ", table_1_sql.count())
            table_1_sql.show(5, False)
