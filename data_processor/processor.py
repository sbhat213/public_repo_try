from pyspark.sql import SparkSession
from injector import inject
from shared.logging.file_logger import FileLogger
from core.repository.model_repository import ModelRepository
from core.repository.process_configuration_repository import ProcessConfigurationRepository
from shared.utils.custom_json import map_pynamo_item_to_python
from data_processor import injector
from shared.dto.model import Model
from shared.dto.step_config import StepConfig
from data_processor.dto.step_param import StepParam
import datetime
import json
import sys
from data_processor.utils.shuffle_partition_calculator import shuffle_partition


class DataProcessor:

    @inject
    def __init__(self, model_repository: ModelRepository, config_repository: ProcessConfigurationRepository):
        self.model_repository = model_repository
        self.config_repository = config_repository

    def create_spark_session(self, no_of_partitions, app_name='run'):
        spark_builder = SparkSession.builder.appName(app_name)
        spark_session = spark_builder.config("spark.jars.packages",
                                             "io.delta:delta-core_2.12:0.7.0") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        spark_session.conf.set("spark.sql.shuffle.partitions", no_of_partitions)
        return spark_session

    def process(self, params):
        model = params["model"]
        process_config = params["process_config"]
        input_path = params["url"]
        last_execution_date = params["last_success_date"]
        stage_path = params["stage_path"]
        process_config = StepConfig(map_pynamo_item_to_python(self.config_repository.get(process_config)))
        model = Model(map_pynamo_item_to_python(self.model_repository.get(model)))
        dependent_models = self.__get_model_configs(process_config.depends_on)

        # To calculate no. of shuffle partitions on basis if file size
        # For transformation job, default value 2 used
        # no_of_partitions = 2

        no_of_partitions = shuffle_partition(self, input_path)
        print("No of shuffle partitions : ",  no_of_partitions)

        # Creating spark session
        spark = self.create_spark_session(no_of_partitions)

        step_param = StepParam(input_path, None, spark, model, dependent_models, process_config,
                               last_execution_date, stage_path)
        for step_config in process_config.steps:
            step_param.step_config = step_config
            step_name = step_config['name']
            print("")
            print("[************ " + step_name + ": Start *************]")
            print(step_name)
            injector.get(step_name).execute(step_param)
            print("[************ " + step_name + ": End ***************]")
        print("No of shuffle partitions used : ",  no_of_partitions)

    def __get_model_configs(self, configs):
        result = {}
        for config in configs:
            model = Model(map_pynamo_item_to_python(self.model_repository.get(config)))
            result[config] = model
        return result


if __name__ == '__main__':
    dp = injector.get(DataProcessor)
    logger = injector.get(FileLogger)
    print(json.dumps(sys.argv[1]))
    print("*********************************")
    params = json.loads(sys.argv[1])

    # params = {}
    # params['last_success_date'] = '2021-02-24 15:05:03'
    # params["stage_path"] = 'D:/share/stage_path'

    # params["model"] = "sphase_monjuvi_speakers"
    # params["process_config"] = "sphase_monjuvi_speakers"
    # params["url"] = "D:/input/files/SPHASE_MONJUVI_SPEAKERS_20200923.txt"

    # params["model"] = "dim_spkrs"
    # params["process_config"] = "dim_spkrs_incremental"
    # params["url"] = ""

    start = datetime.datetime.now()
    dp.process(params)

    print("")
    print("Total Execution Time Taken : ", datetime.datetime.now() - start)
