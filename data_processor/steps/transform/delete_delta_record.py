
from data_processor.utils.step_reader import get_filter
from settings import Settings


class DeleteDeltaRecord:

    def execute(self, params):
        table_name = params.model.table_name

        # Fetching filter column and filter value from config file
        column, value = get_filter(params.step_config)

        # Added for testing purpose
        if Settings.LOG_FLAG['print_show_count']:
            df = params.spark.sql(f"SELECT * from {table_name} ")
            df.show(2, False)
            print("before delete count : ", df.count())

        params.data_frame = params.spark.sql(f"delete from {table_name} WHERE {column} = '{value}'")
        params.data_frame = params.spark.sql(f"SELECT * from {table_name} ")

        if Settings.LOG_FLAG['print_show_count']:
            print("after delete count : ", params.data_frame.count())
            params.data_frame.show(2, False)
