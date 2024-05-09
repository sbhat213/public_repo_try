from pyspark.sql.functions import year, month, col
from data_processor.sql.constant import Map
from data_processor.utils.step_reader import get_partition_key, get_partition_action


class PartitionTable:

    def execute(self, params):
        key = get_partition_key(params.step_config)
        partition_action = get_partition_action(params.step_config)
        if partition_action == Map.PARTITION_DATE:
            params.data_frame = (params.data_frame
                                 .withColumn("year", year(col(key).cast(key)))
                                 .withColumn("month", month(col(key).cast(key))))
