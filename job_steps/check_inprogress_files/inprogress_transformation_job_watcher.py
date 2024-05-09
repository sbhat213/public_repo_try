from injector import inject
from core.entities.job_status import JobStatus
# from core.entities.job_type import JobType
# from core.entities.job_interval import JobInterval
from core.repository.transformation_job_repository import TransformationJobRepository
from settings import Settings
from shared.logging.logger import Logger
from job_steps.dto.check_spark_state import CheckSparkState
from job_steps.dto.task_type import TaskType
from shared.utils.datetime import utc_now,date_to_str
# import os


class InProgressTransformationJobWatcher:

    @inject
    def __init__(self, tj_repository: TransformationJobRepository, settings: Settings, logger: Logger):
        self.settings = settings
        self.tj_repository = tj_repository
        self.limit = settings.Schedule["limit"]
        self.check_limit = settings.Schedule["check_limit"]
        self.logger = logger

    def get_tasks(self, schedule_state):
        logger = self.logger
        tj_list = self.tj_repository.get_inprogress_jobs()
        logger.info(f'IN PROGRESS JOB LIST RETURNED IS {tj_list}')

        schedule_state = self.__updateScheduleState(tj_list, schedule_state, logger, self.check_limit,
                                                    )

        return schedule_state

    def __updateScheduleState(self, tj_list, schedule_state, logger, limit):

        job_list = list(filter(lambda x: x.status == JobStatus.In_Progress.value, tj_list))

        in_progress_jobs = len(job_list)
        if in_progress_jobs > 0:
            logger.info('IN PROGRESS jobs greater than 0')
            schedule_state.css_tasks = self.__map_css_task(job_list[0:in_progress_jobs])

            logger.info(
                f'SCHEDULE STATE UPDATED FOR TJW :- \n available_slots : {schedule_state.available_slots} \n tasks_in_progress : {schedule_state.css_tasks} ')

            return schedule_state

        return schedule_state

    def __map_css_task(self, tasks):
        result = []

        for item in tasks:
            css_task = CheckSparkState(item.model, item.last_execution_detail.response_id,
                                       item.last_execution_detail.livy_url,
                                       item.id, TaskType.Transform.name, item.job_type,
                                       date_to_str(item.updated_on))
            result.append(css_task.__dict__)
        return result
