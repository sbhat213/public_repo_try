import json
from shared.utils.datetime import utc_now
from injector import inject
from shared.livy.livy_client import LivyClient
from settings import Settings
from shared.logging.logger import Logger
import traceback


class Waiter:

    @inject
    def __init__(self, livy: LivyClient, settings: Settings, logger: Logger):
        self.livy = livy
        self.settings = settings
        self.logger = logger

    def check_status_job(self, livyUrl, response_id):

        logger = self.logger
        try:
            response_type, response_state = self.livy.wait(livyUrl, response_id)
            #response_type, response_state = True,  {'id': 2, 'name': None, 'owner': None, 'proxyUser': None, 'state': 'success', 'appId': 'application_1613710865455_0003', 'appInfo': {'driverLogUrl': 'http://ip-10-0-1-34.ap-south-1.compute.internal:8188/applicationhistory/logs/ip-10-0-1-152.ap-south-1.compute.internal:8041/container_1613710865455_0003_01_000001/container_1613710865455_0003_01_000001/livy', 'sparkUiUrl': 'http://ip-10-0-1-34.ap-south-1.compute.internal:20888/proxy/application_1613710865455_0003/'}, 'log': ['21/02/19 05:26:44 INFO BlockManager: BlockManager stopped', '21/02/19 05:26:44 INFO BlockManagerMaster: BlockManagerMaster stopped', '21/02/19 05:26:44 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!', '21/02/19 05:26:44 INFO SparkContext: Successfully stopped SparkContext', '21/02/19 05:26:44 INFO ShutdownHookManager: Shutdown hook called', '21/02/19 05:26:44 INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-a2ed322b-20df-4329-b45f-eb70f040eae9', '21/02/19 05:26:44 INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-f644f629-44f4-4215-8c2c-c5e2e714d3d6', '21/02/19 05:26:44 INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-f644f629-44f4-4215-8c2c-c5e2e714d3d6/pyspark-8b5642ba-40f1-43c5-8e94-be5e9e174bc2', '\nstderr: ', '\nYARN Diagnostics: ']}

            if response_type:
                logger.info(f'State returned by waiter for response_id {response_id}  is {response_state["state"]}')
            if not response_type:
                logger.info(f'State returned by waiter for response_id {response_state}')
            return response_type, response_state

        except Exception as ex:
            logger.info(f'{ex.args[0]} Exception in waiter for response_id {response_id} ')
            logger.info(traceback.format_exc())

    def check_redshift_status_job(self, updated_on):
        rds_state = True

        if int(utc_now() - updated_on / 60) > 15:
            rds_state = False
        return rds_state
