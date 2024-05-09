from data_processor.utils.step_reader import *
from injector import inject
from pyspark.sql.functions import sha2, concat_ws


class AddHash:

    def execute(self, params):
        field = "row_hash"
        columns = self.__get_columns(params.model)
        params.data_frame = params.data_frame.withColumn(field, sha2(concat_ws(" ", *columns), 256))

    def __get_columns(self, model):
        columns = []
        for field in model.fields:
            if field.core_field:
                columns.append(field.name)
        return columns
