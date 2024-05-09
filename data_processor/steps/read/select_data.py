from data_processor.sql.merge_mapper import MergeMapper
from injector import inject
from data_processor.utils.step_reader import *
from settings import Settings


class SelectData:

    @inject
    def __init__(self, mapper: MergeMapper):
        self.mapper = mapper

    def execute(self, params):
        table_name = params.model.table_name
        self.mapper.last_execution_date = params.last_execution_date
        where_condition = ""
        where_map = get_where_map(params.step_config)
        if where_map != "":
            where_params = self.mapper.get_sql_join_filter_map(where_map, table_name, table_name)
            where_condition = f"where {where_params}"

        params.data_frame = params.spark.sql(f"SELECT * from {table_name} {where_condition}")

        # Added for testing purpose
        if Settings.LOG_FLAG['print_show_count']:
            print("Select Query : ", f"SELECT * from {table_name} {where_condition}")
            print("Select Count : ", params.data_frame.count())
            params.data_frame.show(5, False)
