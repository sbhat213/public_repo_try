from job_steps import injector
from shared.logging.logger import Logger
from job_steps.check_available_files.file_watcher import FileWatcher
from job_steps.check_available_files.transformation_job_watcher import TransformationJobWatcher
from job_steps.dto.schedule_state import ScheduleState
from job_steps.check_available_files import STEP_NAME


def lambda_handler(event, context):
    logger = injector.get(Logger)
    file_watcher = injector.get(FileWatcher)
    tj_watcher = injector.get(TransformationJobWatcher)
    schedule_state = ScheduleState()

    try:
        file_watcher.get_tasks(schedule_state)
        if schedule_state.available_slots > 0:
            tj_watcher.get_tasks(schedule_state)
        logger.info(f'Returning schedule state DICT {schedule_state.__dict__}')
        return schedule_state.__dict__

    except Exception as e:
        logger.error(f"At {STEP_NAME} Exception generated is  {e.args[0]}")
        raise e

#
# lambda_handler('', '')

