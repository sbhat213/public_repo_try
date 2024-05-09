from injector import inject
from data_processor.utils.spark_helper import SQLHelper
from settings import Settings


class ReadDeltaTable:

    @inject
    def __init__(self, helper: SQLHelper):
        self.helper = helper

    def execute(self, params):
        path = params.model.path
        table_name = params.model.table_name
        self.helper.create_delta_table(params.spark, table_name, path, params.model)
        params.data_frame = params.spark.sql(f"SELECT * from {table_name} ")

        # Added for testing purpose
        if Settings.LOG_FLAG['print_show_count']:
            params.data_frame.show(5, False)
