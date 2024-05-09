from job_steps.wait_run_spark_job import STEP_NAME
from shared.logging.logger import Logger
# from core.entities.job_type import JobType
from job_steps.dto.check_spark_state import CheckSparkState
from settings import Settings
from job_steps import injector
import traceback
import json


def lambda_handler(event, context):
    try:
        logger = injector.get(Logger)
        settings = injector.get(Settings)
        data = event
        logger.info(f"For {STEP_NAME} Input event is : {event}")
        check_spark_state = CheckSparkState()
        check_spark_state.set_values(data)
        logger.info("check spark_state object created successfully")
        injector.get(check_spark_state.task_type).run(check_spark_state)
        logger.info(f"Returning check_spark_state values {check_spark_state.__dict__} ")
        return check_spark_state.__dict__

    except Exception as e:
        logger.info(traceback.format_exc())
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
        raise e
#
# event_json = {
#     "model": "dim_spkr_programs",
#     "response_id": 2,
#     "livy_url": "http://ip-10-0-1-34.ap-south-1.compute.internal:8998/batches",
#     "task_id": "dim_spkr_programs_daily",
#     "task_type": "Transform",
#     "job_type": 1,
#     "updated_on": "2021-02-19 05:25:43 UTC",
#     "status": "",
#     "error_code": "",
#     "error": "",
#     "application_id": ""
# }
# lambda_handler(event_json,'')
