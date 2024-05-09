import os

from core.command.add_file_command import AddFileCommand
from core.command.handler.add_file_handler import AddFileHandler
from job_steps import injector
from shared.logging.logger import Logger
from shared.utils import custom_json
from job_steps.save_source_file import STEP_NAME
import traceback


def lambda_handler(event, scontext):
    logger = injector.get(Logger)
    message = event['Records'][0]['s3']
    logger.info(f"For {STEP_NAME} Input event is : {event}")
    try:
        bucket = message['bucket']['name']
        path = message['object']['key']
        key = os.path.basename(path)
        location = os.path.dirname(path)
        url = os.path.join("s3://", bucket, location)
        logger.info("Metadata step execution started for the file {} from url {}".format(key, url))
        file_handler = injector.get(AddFileHandler)
        command = AddFileCommand(key, url, STEP_NAME)
        file_handler.handle(command)
        logger.info("Metadata step execution completed for the file {} from url {}".format(key, url))
    except Exception as e:
        logger.info(traceback.format_exc())
        logger.error(f"At {STEP_NAME} Exception generated is {e.args[0]}")
