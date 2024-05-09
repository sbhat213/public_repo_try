from job_steps.kill_long_running_jobs import STEP_NAME
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
