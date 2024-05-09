
from pyspark.sql import functions as f
from settings import Settings


class RemoveDuplicate:

    def execute(self, params):
        # Trimming spaces from all columns
        for colname in params.data_frame.columns:
            params.data_frame = params.data_frame.withColumn(colname, f.trim(f.col(colname)))

        # Removing Duplicate record based on composite key
        col = params.model.composite_key

        if Settings.LOG_FLAG['print_show_count']:
            print("Count before drop duplicate on composite key : " + str(col) + " ,Count : " + str(params.data_frame.count()))

        params.data_frame = params.data_frame.dropDuplicates(col)

        if Settings.LOG_FLAG['print_show_count']:
            print("Count after drop duplicate : " + str(params.data_frame.count()))
