from pyspark.sql import functions as F
from uuid import uuid4
from data_processor.sql.constant import UDF


def register_udf(spark):
    register_unique_id(spark)


def register_unique_id(spark):
    unique_id = F.udf(lambda: unique_id())
    spark.udf.register(UDF.UUID_NAME, unique_id.asNondeterministic())


def unique_id():
    return str(uuid4())
