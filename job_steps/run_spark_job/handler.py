from job_steps import injector
from job_steps.run_spark_job import STEP_NAME
from shared.logging.logger import Logger
from job_steps.dto.spark_state import SparkState
from settings import Settings
import traceback


def lambda_handler(event, context):
    logger = injector.get(Logger)

    try:

        settings = injector.get(Settings)
        data = event

        logger.info(f"For {STEP_NAME} Input event is : {event}")
        spark_state = SparkState()
        spark_state.set_values(data)
        logger.info("spark_state object created successfully")
        logger.info(
            f'Calling Run with {spark_state.task.task_type} and {spark_state.__dict__} and {injector.get(spark_state.task.task_type)}')
        response_id, livy_url = injector.get(spark_state.task.task_type).run(spark_state)
        spark_state.response_id = response_id
        logger.info(f'SPARK STATE UPDATED \n response_id : {spark_state.response_id}')
        logger.info(f"Running task of type {spark_state.task.task_type} ")
        spark_state.task = spark_state.task.__dict__
        return spark_state.__dict__

    except Exception as e:
        logger.info(traceback.format_exc())
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
        raise e
#
#
# input_json = {
#     "task": {
#         "task_type": "Transform",
#         "process_config": "customer_master_incremental",
#         "model": "customer_master",
#         "task_id": "customer_master_daily",
#         "url": "",
#         "job_type": 1,
#         "last_success_date": "2021-01-28 11:37:33.588271+00:00",
#         "stage_path": ""
#     },
#     "livyUrl": "http://ip-10-0-1-9.ap-south-1.compute.internal:8998/batches",
#     "response_id": ""
# }
#
# lambda_handler(input_json, '')
