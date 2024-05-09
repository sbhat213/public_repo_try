from injector import inject
from shared.http.request import Requests
import time
import json
from settings import Settings
from shared.http.http_status import HttpStatus
from shared.logging.logger import Logger
import traceback


class LivyClient:

    @inject
    def __init__(self, requests: Requests, logger: Logger):
        self.requests = requests
        self.logger = logger
        self.session_states = {
            "error",
            "dead",
            "killed",
            "success",
        }

    def run_batch_wait(self, url, params):
        logger = self.logger
        headers = {"content-type": "application/json"}
        data = self.requests.post(f"{url}", params, headers=headers)
        if data.status_code == HttpStatus.created_201.value:
            response = json.loads(data.content)
            logger.info(f'Response returned by wait in RUN BATCH WAIT is {json.loads(data.content)}')
        else:
            raise Exception("Failed to create spark job")
        # return self.__wait(url, response["id"])
        return response

    def session_kill(self, url, batch_id):
        logger = self.logger
        headers = {"content-type": "application/json"}
        response_kill = self.__kill_session(url, batch_id)

        if response_kill.status_code == HttpStatus.ok_200.value:
            logger.info(f'Response returned by kill request in SESSION KILL '
                        f'REQUEST is {json.loads(response_kill.content)}')
        else:
            raise Exception("Failed to Kill spark job")

        return response_kill.status_code

    def wait(self, url, batch_id):
        logger = self.logger

        logger.info(f'IN LIVY WAIT FUNCTION')
        data = self.__get_state(url, batch_id)
        logger.info(f'DATA STATE RETUNED IS {data}')
        if data.status_code == HttpStatus.ok_200.value:
            state = json.loads(data.content)["state"]
            logger.info(
                f'Final State Response returned for {batch_id} by wait in WAITER is : \n '
                f' {json.loads(data.content)} \n with state = {state}')
            final_response = json.loads(data.content)
            response_logs = self.__get_logs(url, batch_id)
            logger.info(f'RESPONSE LOGS FOR  {batch_id} IS \n {response_logs.text}')
            return_state = True
            return return_state, final_response
        else:
            logger.info(f'DATA STATE RETUNED IS {json.loads(data.content)}')
            final_response = json.loads(data.content)
            return_state = False
        logger.info(f'Final State Response returned by wait in WAITER is {return_state} ,'
                    f'\n  {final_response}')

        return return_state, final_response

    def __kill_session(self, url, batch_id):
        response = self.requests.delete(f"{url}/{batch_id}")
        return response

    def __get_state(self, url, batch_id):
        batch = self.requests.get(f"{url}/{batch_id}")
        return batch

    def __get_logs(self, url, batch_id):
        fetch_log_size = Settings.CLUSTER_DETAILS["livy_conf_params"]["fetch_log_size"]
        payload = {'from': '0', 'size': fetch_log_size}
        batch = self.requests.get(f"{url}/{batch_id}/log", params=payload)
        return batch
