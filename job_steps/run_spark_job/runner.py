import json
from injector import inject
from shared.livy.livy_client import LivyClient
from settings import Settings
from shared.logging.logger import Logger
# from job_steps import injector
import traceback


class Runner:

    @inject
    def __init__(self, livy: LivyClient, settings: Settings, logger: Logger):
        self.livy = livy
        self.settings = settings
        self.logger = logger

    def run_job(self, livyUrl, args):

        try:
            logger = self.logger
            # logger = injector.get(Logger)

            bucket = self.settings.s3_staging_files["bucket"]
            dir_prefix = self.settings.s3_staging_files["staging_prefix"].format(args.model)
            args.stage_path = f"s3://{bucket}/{dir_prefix}"

            logger.info(f'ARGS received is {args.__dict__}')
            params = {
                "file": self.settings.SPARK["file"],
                "pyFiles": self.settings.SPARK["pyFiles"],
                "jars": self.settings.SPARK["jars"],
                "args": [json.dumps(args.__dict__)],
                "conf": self.settings.SPARK["conf"],

                # "executorCores": self.settings.SPARK["executorCores"],
                # "driverCores": self.settings.SPARK["driverCores"],
                # "executorMemory": self.settings.SPARK["executorMemory"],
                # "driverMemory": self.settings.SPARK["driverMemory"],
                # "numExecutors": self.settings.SPARK["numExecutors"],
                # "queue": self.settings.SPARK["queue"]
                    }

            response = self.livy.run_batch_wait(livyUrl, params)
            logger.info(f'Response Received  for batch submissions is {response} ')
            return response['id'], livyUrl

        except Exception as ex:
            logger.info(f'{ex.args[0]} Exception ')
            logger.info(traceback.format_exc())
