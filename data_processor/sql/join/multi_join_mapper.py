from data_processor.sql.join.single_join_mapper import SingleJoinMapper
from data_processor.sql.merge_mapper import MergeMapper
from data_processor.utils.step_reader import *
from pyspark.sql.functions import col
from injector import inject
from settings import Settings


class MultiJoinMapper(SingleJoinMapper):
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
        # print("insert_query : " + insert_query)
        # print("update_query : " + update_query)
        # print("filter_query : " + filter_query)
        # print("join_query   : " + join_query)
        return insert_query, update_query, filter_query, join_query, table_1

    def get_join_query(self, params):
        join_map = get_join_map(params.step_config)
        tables = join_map["tables"]
        table_2 = join_map["with"]
        for table_obj in tables:
            table = table_obj["table"]
            update_model = params.dependent_models[table]
            params.spark.sql(
                "CREATE TABLE {} USING DELTA LOCATION {}".format(table, "'" + str(update_model.path) + "'"))

        table_left = tables[0]
        table_right = tables[1]
        table_left_name = table_left["table"]
        table_right_name = table_right["table"]
        table_left_fields = table_left["field"]
        table_right_fields = table_right["field"]
        join_operator = join_map["join_operator"]
        condition = f""

        # Preparing Joining condition
        for index, (table_left_field, table_right_field) in enumerate(zip(table_left_fields, table_right_fields)):
            if index == (len(table_left_fields) - 1):
                condition += f"({table_left_name}.{table_left_field} = {table_right_name}.{table_right_field})"
            else:
                condition += f"({table_left_name}.{table_left_field} = " \
                             f"{table_right_name}.{table_right_field}) {join_operator} "
        # print(condition)

        # Selecting data from left and right table
        left_table = params.spark.sql(f"SELECT  * from {table_left_name}")
        right_table = params.spark.sql(f"SELECT  * from {table_right_name}")

        # Added for testing - Remove after test
        if Settings.LOG_FLAG['print_show_count']:
            print("Left Table Name: " + table_left_name, left_table.count())
            left_table.show()
            print("Right Table Name: " + table_right_name, right_table.count())
            right_table.show()

        # Iterating for selecting right table columns which is not in left table
        left_table_col = left_table.columns
        right_table_col = right_table.columns

        right_col_not_in_left = []
        for i in range(len(right_table_col)):
            if right_table_col[i] not in left_table_col:
                right_col_not_in_left.append(right_table_col[i])

        if Settings.LOG_FLAG['print_show_count']:
            print("Left table column : " + str(left_table_col))
            print("Right table column : " + str(right_table_col))
            print("Right table column which is not in left table : " + str(right_col_not_in_left))

        join_df = params.spark.sql(f"Select * FROM {table_left_name} AS {table_left_name} "
                                   f"LEFT JOIN {table_right_name} AS {table_right_name} "
                                   f"ON {condition} ")

        print(f"Select * FROM {table_left_name} AS {table_left_name} "
              f"LEFT JOIN {table_right_name} AS {table_right_name} ON {condition} ")

        if Settings.LOG_FLAG['print_show_count']:
            print("Count - After Join ", join_df.count())
            join_df.show(50, False)

        table_2_select = join_df.select([col(f"{table_left_name}." + colname) for colname in left_table_col] +
                                        [col(f"{table_right_name}." + colname) for colname in right_col_not_in_left])

        # Checking for duplicate record after join on left table primary key
        # If duplicate_check is true, then filter data whose count > 1
        # If duplicate_check is false, return full data

        duplicate_check = join_map["duplicate_check"]
        if duplicate_check:
            return self.remove_duplicate(params, table_left_name, table_2_select, table_2)
        else:
            table_2_select.createOrReplaceTempView(table_2)

        return table_2

    @staticmethod
    def remove_duplicate(params, table_left_name, table_2_select, table_2):
        # Fetching primary key of left table from model
        left_table_model = params.dependent_models[table_left_name]
        key = left_table_model.primary_key

        table_2_select.createOrReplaceTempView("t1")
        table_dup = params.spark.sql(f"Select *, count(*) over (partition by {key}) row_count from t1;")

        table_3_select = table_dup.filter(table_dup.row_count == 1)

        # Added for Testing which data get filtered out
        if Settings.LOG_FLAG['print_show_count']:
            print("data which count is equal to 1 ")
            table_3_select.show(50, False)

            table_drop = table_dup.filter(table_dup.row_count >= 2)
            print("date which get dropped >= 2 ")
            table_drop.show(50, False)

        table_3_select.createOrReplaceTempView(table_2)
        return table_2
