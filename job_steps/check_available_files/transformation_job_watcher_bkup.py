from injector import inject
from core.entities.job_status import JobStatus
from core.entities.job_type import JobType
from core.entities.job_interval import JobInterval
from core.repository.transformation_job_repository import TransformationJobRepository
from settings import Settings
from shared.logging.logger import Logger
from job_steps.dto.task import Task
from job_steps.dto.task_type import TaskType
from shared.utils.datetime import utc_now
import os


class TransformationJobWatcherBkup:

    @inject
    def __init__(self, tj_repository: TransformationJobRepository, settings: Settings, logger: Logger):
        self.settings = settings
        self.tj_repository = tj_repository
        self.limit = settings.Schedule["limit"]
        self.rds_limit = settings.Schedule["rds_limit"]

        self.logger = logger

    def get_tasks(self, schedule_state):
        logger = self.logger
        tj_list = self.tj_repository.get_active_jobs()
        logger.info(f'ACTIVE JOB LIST RETURNED IS {tj_list}')
        tj_list = self.__time_range_jobs(tj_list)
        logger.info(f'FILTERED TIME RANGE JOBS  {tj_list}')
        tj_list_spark, tj_list_rds = self.__jobs_differentiator(tj_list)
        logger.info(f'tj_list_spark is : {tj_list_spark} \n tj_list_rds is {tj_list_rds}')
        if len(tj_list_rds) > 0:
            logger.info(f'Calling updateScheduleState for redshift Tasks')
            schedule_state = self.__updateScheduleState(tj_list_rds, schedule_state, logger, self.rds_limit,
                                                        JobType.Rds.value)

        if len(tj_list_spark) > 0:
            logger.info(f'Calling updateScheduleState for spark Tasks')
            schedule_state = self.__updateScheduleState(tj_list_spark, schedule_state, logger, self.limit,
                                                        JobType.Spark.value)

        return schedule_state

    def __updateScheduleState(self, tj_list, schedule_state, logger, limit, job_type):

        job_list = list(filter(lambda x: x.status == JobStatus.In_Progress.value, tj_list))

        in_progress_jobs = len(job_list)
        if in_progress_jobs > 0:
            logger.info('IN PROGRESS jobs greater than 0')
            schedule_state.create_cluster = True
            schedule_state.tasks_in_progress = True
            logger.info(
                f'SCHEDULE STATE UPDATED FOR TJW :- \n create_cluster : {schedule_state.create_cluster} \n tasks_in_progress : {schedule_state.tasks_in_progress} ')

            if in_progress_jobs >= limit:
                logger.info('IN PROGRESS jobs is more than LIMIT , So No Available Slots in FOR TJW')
                schedule_state.available_slots = 0
                logger.info(
                    f'SCHEDULE STATE UPDATED FOR TJW :- \n available_slots : {schedule_state.available_slots} ')

                return schedule_state

        # Check other jobs

        job_list = list(filter(lambda x: x.status == JobStatus.Ready_To_Schedule.value, tj_list))

        if len(job_list) > 0:
            logger.info('In FOR TJW READY TO SCHEDULE jobs is greater than 0')
            schedule_state.tasks = schedule_state.tasks + (
                self.__map_task(job_list[0:schedule_state.available_slots - in_progress_jobs]))
            schedule_state.create_cluster = True
            logger.info(
                f'FOR TJW SCHEDULE STATE UPDATED :- \n create_cluster : {schedule_state.create_cluster} \n  tasks : {schedule_state.tasks}')

            return schedule_state
        for els in tj_list:
            print(els.is_dependent)

        job_list = list(filter(lambda x: x.status  == JobStatus.Created.value , tj_list))
        job_list = list(filter(lambda x:  x.is_dependent ==  False, job_list))
        logger.info(f'CREATED JOBS IS DEPENDENT FALSE {job_list}')
        if len(job_list) > 0:
            logger.info('FOR TJW ,CREATED jobs is greater than 0')
            schedule_state.tasks = schedule_state.tasks + (
                    self.__map_task(job_list[0:schedule_state.available_slots - in_progress_jobs]))
            schedule_state.create_cluster = True
            logger.info(
                    f'FOR TJW SCHEDULE STATE UPDATED :- \n create_cluster : {schedule_state.create_cluster} \n  tasks : {schedule_state.tasks}')

            return schedule_state
        return schedule_state



    def __map_task(self, tasks):
        result = []

        for item in tasks:
            task = Task(item.model, TaskType.Transform.name, item.process_config, item.id, "", item.job_type,
                        str(item.last_success_date))
            result.append(task.__dict__)
        return result

    def __time_range_jobs(self, tasks):

        for item in tasks:
            if item.frequency["schedule_interval"] == JobInterval.monthly:
                datediffs = utc_now() - item.last_success_date
                if datediffs.days < 30:
                    tasks.remove(item)

            elif item.frequency["schedule_interval"] == JobInterval.Weekly:
                datediffs = utc_now() - item.last_success_date
                if datediffs.days < 7:
                    tasks.remove(item)
        return tasks

    def __jobs_differentiator(self, tj_list):
        logger = self.logger
        tj_list_spark = []
        tj_list_rds = []
        logger.info(f'Length of tj_list is {len(tj_list)}')

        for item in tj_list:
            logger.info(f'ITEM IN TJ_LIST IS {item} and {item.job_type}  ')
            if item.job_type == JobType.Spark.value:
                tj_list_spark.append(item)
            elif item.job_type == JobType.Rds.value:
                tj_list_rds.append(item)
        return tj_list_spark, tj_list_rds
