from pyspark.sql import DataFrame
from settings import Settings


class Validate:

    def execute(self, params):
        # print("spark session", params.spark)
        # print("spark config", params.spark._sc.getConf().getAll())
        print("Model Name : ", params.model.name)
        # params.data_frame.printSchema()
        params.data_frame = DataFrame(
            params.spark._sc._jvm.rules.applyDroolRules(params.data_frame._jdf,
                                                        "com.indegene.business.rules", params.model.name),
            params.spark._wrapped)

        # Added for testing purpose
        if Settings.LOG_FLAG['print_show_count']:
            params.data_frame.show(2, False)
