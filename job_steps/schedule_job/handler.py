from job_steps import injector
from job_steps.dto.schedule_state import ScheduleState
from shared.logging.logger import Logger
from job_steps.dto.spark_state import SparkState
from settings import Settings
import json
import boto3
from job_steps.schedule_job import STEP_NAME


def lambda_handler(event, context):
    logger = injector.get(Logger)
    settings = injector.get(Settings)
    data = event["file_watcher_result"]
    logger.info(f"For {STEP_NAME} Input event is : {event}")
    schedule_state = ScheduleState()
    schedule_state.set_values(data)
    try:
        for task in schedule_state.tasks:
            spark_state = SparkState()
            spark_state.task = task
            spark_state.livyUrl = schedule_state.emr_cluster['livy_url']
            logger.info(
                f'SPARK STATE UPDATED :- \n task : {spark_state.task} \n livyUrl : {spark_state.livyUrl}')
            client = boto3.client('stepfunctions')
            logger.info(
                f'STARTING EXECUTION FOR SPARK STATE :- \n  {spark_state.__dict__}')
            client.start_execution(
                stateMachineArn=settings.SPARK_STEP_FUNCTION,
                input=json.dumps(spark_state.__dict__)
            )
    except Exception as e:
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
        raise e
