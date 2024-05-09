from job_steps import injector
from job_steps.dto.status_check_schedule_state import StatusScheduleState
from shared.logging.logger import Logger
from job_steps.dto.check_spark_state import CheckSparkState
from settings import Settings
import json
import boto3
from job_steps.check_scheduled_jobs import STEP_NAME


def lambda_handler(event, context):
    logger = injector.get(Logger)
    settings = injector.get(Settings)
    data = event["in_progress_watcher_result"]
    logger.info(f"For {STEP_NAME} Input event is : {event}")
    schedule_state = StatusScheduleState()
    schedule_state.set_values(json.loads(data))
    try:
        for css_task in schedule_state.css_tasks:
            check_spark_state = CheckSparkState()
            check_spark_state.set_values(css_task)

            logger.info(
                f'CHECK SPARK STATE UPDATED :- \n task : {check_spark_state.task_id} \n livyUrl : {check_spark_state.livy_url}')
            client = boto3.client('stepfunctions')
            logger.info(
                f'STARTING EXECUTION FOR SPARK STATE :- \n  {check_spark_state.__dict__}')
            client.start_execution(
                stateMachineArn=settings.SPARK_STATUSCHECK_STEP_FUNCTION,
                input=json.dumps(check_spark_state.__dict__)
            )
    except Exception as e:
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
        raise e
