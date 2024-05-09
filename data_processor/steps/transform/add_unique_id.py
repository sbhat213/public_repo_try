
from pyspark.sql import functions as f
from settings import Settings


class AddUniqueId:

    def execute(self, params):

        field = "unique_id"
        params.data_frame = params.data_frame.withColumn(field, f.expr("uuid()"))

        # Added for testing purpose
        # if Settings.LOG_FLAG['print_show_count']:
        #     params.data_frame.show(2, False)
        #     print("Unique Id added with column name : unique_id")
