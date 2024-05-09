import json
from job_steps import injector
from shared.logging.logger import Logger
from job_steps.check_inprogress_files.inprogress_file_watcher import InProgressFileWatcher
from job_steps.check_inprogress_files.inprogress_transformation_job_watcher import InProgressTransformationJobWatcher
from job_steps.dto.status_check_schedule_state import StatusScheduleState
from job_steps.check_inprogress_files import STEP_NAME


def lambda_handler(event, context):
    logger = injector.get(Logger)
    file_watcher = injector.get(InProgressFileWatcher)
    tj_watcher = injector.get(InProgressTransformationJobWatcher)
    schedule_state = StatusScheduleState()

    try:

        file_watcher.get_tasks(schedule_state)
        if len(schedule_state.css_tasks) == 0:
            tj_watcher.get_tasks(schedule_state)
        logger.info(f'Returning schedule state DICT {schedule_state.__dict__}')
        return json.dumps(schedule_state.__dict__)

    except Exception as e:
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
        raise e


# lambda_handler('', '')
