from injector import inject

from data_processor.sql.schema_mapper import SchemaMapper


class SQLHelper:

    @inject
    def __init__(self, mapper: SchemaMapper):
        self.mapper = mapper

    def create_delta_table(self, spark, table_name, path, model):
        # TODO: look for alternate to check if delta table exists
        try:
            spark.sql("CREATE TABLE {} USING DELTA LOCATION {}".format(table_name, "'" + str(path) + "'"))
        except Exception as ex:
            data_frame = spark.createDataFrame([], self.mapper.get_model_schema(model))
            if model.partition_key != "":
                data_frame.write.format("delta").save(path)
            else:
                data_frame.write.partitionBy(model.partition_key).format("delta").save(path)
            spark.sql("CREATE TABLE {} USING DELTA LOCATION {}".format(table_name, "'" + str(path) + "'"))
