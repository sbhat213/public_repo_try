
from data_processor.sql.join.base_join_mapper import BaseJoinMapper
from data_processor.sql.merge_mapper import MergeMapper
from data_processor.utils.step_reader import *
from injector import inject
from settings import Settings


class SingleJoinMapper(BaseJoinMapper):

    @inject
    def __init__(self, mapper: MergeMapper):
        self.mapper = mapper
        super().__init__(mapper)

    def join(self, params):
        self.mapper.last_execution_date = params.last_execution_date
        table_1 = params.model.table_name
        params.data_frame.createOrReplaceTempView(table_1)

        # set filter map
        filter_map = get_filter_map(params.step_config)

        # set field map
        field_map = {}
        step_map = get_step_map(params.step_config)
        field_map.update(step_map)
        skip_global_map = get_skip_global_map(params.step_config)
        print("skip_global_map : " + str(skip_global_map))

        if not skip_global_map:
            field_map.update(params.process_config.global_map.core)
            field_map.update(params.process_config.global_map.audit)
        else:
            field_map.update(params.process_config.global_map.audit)

        # set matched map
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

        filter_query = self.get_filter_query(filter_map)
        join_query = self.get_join_query(params)

        # print("")
        # print("insert_query : " + insert_query)
        # print("update_query : " + update_query)
        # print("filter_query : " + filter_query)
        # print("join_query   : " + join_query)
        # print("")

        return insert_query, update_query, filter_query, join_query, table_1

    def get_join_query(self, params):
        join_map = get_join_map(params.step_config)
        depends_on = get_join_with(join_map)
        update_model = params.dependent_models[depends_on]
        table_2 = update_model.table_name
        params.spark.sql("CREATE TABLE {} USING DELTA LOCATION {}".format(table_2, "'" + str(update_model.path) + "'"))

        # Adding filter condition to select latest data based on last_execution_date
        self.mapper.last_execution_date = params.last_execution_date
        where_map = get_where_map(params.step_config)
        if where_map != "":
            where_params = self.mapper.get_sql_join_filter_map(where_map, table_2, table_2)
            where_condition = f"where {where_params}"
            print("Table on which * where * operation performed : " + str({table_2}))
            table_2_sql = params.spark.sql(f"SELECT * from {table_2} {where_condition}")
            print("Table_2 select query : " + f"SELECT * from {table_2} {where_condition}")
            table_2 = table_2 + "_where"
            table_2_sql.createOrReplaceTempView(table_2)

        # Adding duplicate removal condition to remove duplicate record based on specified column
        duplicate_map = get_duplicate_map(params.step_config)
        if duplicate_map != "":
            columns = duplicate_map["columns"]
            dup = []
            for field_obj in columns:
                dup.append(field_obj["field"])

            print("Table on which * Duplicate * operation performed : " + str({table_2}))
            table_dup = params.spark.sql(f"SELECT * from {table_2}")

            if Settings.LOG_FLAG['print_show_count']:
                print("Count before drop duplicate on specified column : " + str(dup) + " ,Count : " + str(table_dup.count()))
            table_dup = table_dup.dropDuplicates(dup)

            if Settings.LOG_FLAG['print_show_count']:
                print("Count after drop duplicate : " + str(table_dup.count()))
            table_2 = table_2 + "_dup"
            table_dup.createOrReplaceTempView(table_2)

        return table_2
