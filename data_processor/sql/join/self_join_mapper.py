from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from data_processor.sql.join.base_join_mapper import BaseJoinMapper
from data_processor.sql.merge_mapper import MergeMapper
from data_processor.utils.spark_helper import SQLHelper
from data_processor.utils.step_reader import *
from injector import inject


class SelfJoinMapper(BaseJoinMapper):

    @inject
    def __init__(self, mapper: MergeMapper, helper: SQLHelper):
        self.mapper = mapper
        self.helper = helper
        super().__init__(mapper)

    def join(self, params):
        alias = "temp_merge"
        params.data_frame.createOrReplaceTempView(alias)
        filter_map = get_filter_map(params.step_config)
        field_map = {}
        step_map = get_step_map(params.step_config)

        for field in params.model.fields:
            if field.name not in step_map:
                child = {"field": field.name}
                field_map[field.name] = child

        field_map.update(step_map)
        matched_map = get_matched_map(params.step_config)

        skip_insert = get_skip_insert(params.step_config)
        print("skip_insert : " + str(skip_insert))
        if not skip_insert:
            insert_query = self.get_insert_query(field_map)
        else:
            insert_query = ""

        skip_update = get_skip_update(params.step_config)
        print("skip_update : " + str(skip_update))
        if not skip_update:
            update_query = self.get_update_query(matched_map, field_map)
        else:
            update_query = ""
        # update_query = self.get_update_query(matched_map, field_map)

        filter_query = self.get_filter_query(filter_map)
        join_query = self.__get_join_query(params)

        # print("insert_query : " + insert_query)
        # print("update_query : " + update_query)
        # print("filter_query : " + filter_query)
        # print("join_query   : " + join_query)

        return insert_query, update_query, filter_query, alias, join_query

    def __get_join_query(self, params):
        table_2 = params.model.table_name
        table_2 = table_2 + "_update"
        # table_2_sql = params.spark.sql(f"SELECT  * from {table_2}")
        # table_2_sql.createOrReplaceTempView(table_2)
        # print("---Self Join data---" + table_2)
        # print(table_2_sql.show())
        self.helper.create_delta_table(params.spark, table_2, params.model.path, params.model)
        return table_2
