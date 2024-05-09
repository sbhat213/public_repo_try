import json
from shared.utils.datetime import utc_now
from injector import inject
from shared.livy.livy_client import LivyClient
from settings import Settings
from shared.logging.logger import Logger
import traceback


class Killsession:

    @inject
    def __init__(self, livy: LivyClient, settings: Settings, logger: Logger):
        self.livy = livy
        self.settings = settings
        self.logger = logger

    def kill_job(self, livyUrl, response_id):

        logger = self.logger
        try:
            response_state = self.livy.session_kill(livyUrl, response_id)
            logger.info(f'STATE RETURNED BY KILL SESSION IS {response_state}')
            return response_state
        except Exception as ex:
            logger.info(f'{ex.args[0]} Exception in Killsession for response_id {response_id} ')
            logger.info(traceback.format_exc())
