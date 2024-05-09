from data_processor.utils.step_reader import get_separator
from pyspark.sql import functions as f
from settings import Settings


class ReadCSV:

    def execute(self, params):
        separator = get_separator(params.step_config)
        params.data_frame = params.spark.read.csv(params.input_path, sep=separator, inferSchema=True, header=True,
                                                  mode="DROPMALFORMED")
        # Replacing space with "_" from column name
        params.data_frame = params.data_frame.select([f.col(col).alias(col.replace(' ', '_'))
                                                      for col in params.data_frame.columns])
        params.data_frame = params.data_frame.select([f.col(col).alias(col.replace('(', ''))
                                                      for col in params.data_frame.columns])
        params.data_frame = params.data_frame.select([f.col(col).alias(col.replace(')', ''))
                                                      for col in params.data_frame.columns])
        # lowering the column name
        params.data_frame = params.data_frame.select([f.col(col).alias(col.lower()) for col in params.data_frame.columns])

        if Settings.LOG_FLAG['print_show_count']:
            print("Read_CSV Count : ", params.data_frame.count())
            params.data_frame.show(2, False)
