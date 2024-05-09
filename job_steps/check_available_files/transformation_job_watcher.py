from injector import inject
from core.entities.job_status import JobStatus
from core.entities.job_interval import JobInterval
from core.repository.transformation_job_repository import TransformationJobRepository
from settings import Settings
from shared.logging.logger import Logger
from job_steps.dto.task import Task
from job_steps.dto.task_type import TaskType
from shared.utils.datetime import utc_now, utc_date_diff_in_hour, date_diffrence_days, \
    date_diffrence_hour, \
    date_diffrence_minutes, date_diffrence_seconds
import pytz


class TransformationJobWatcher:

    @inject
    def __init__(self, tj_repository: TransformationJobRepository, settings: Settings, logger: Logger):
        self.settings = settings
        self.tj_repository = tj_repository
        self.limit = settings.Schedule["limit"]
        self.logger = logger
        self.retry_count = settings.Schedule["retry_count"]

    def get_tasks(self, schedule_state):
        logger = self.logger
        tj_list = self.tj_repository.get_active_jobs(self.retry_count)
        logger.info(f'ACTIVE JOB LIST RETURNED IS {tj_list}')
        tj_list = self.__time_range_jobs(tj_list)
        logger.info(f'FILTERED TIME RANGE JOBS  {tj_list}')
        schedule_state = self.__updateScheduleState(tj_list, schedule_state, logger, self.limit)

        return schedule_state

    def __updateScheduleState(self, tj_list, schedule_state, logger, limit):

        job_list = list(filter(lambda x: x.status == JobStatus.In_Progress.value, tj_list))
        in_progress_jobs = len(job_list)
        if in_progress_jobs > 0:
            logger.info('IN PROGRESS jobs greater than 0')
            schedule_state.create_cluster = True
            schedule_state.tasks_in_progress = True
            logger.info(
                f'SCHEDULE STATE UPDATED FOR TJW :- \n create_cluster : {schedule_state.create_cluster}'
                f' \n tasks_in_progress : {schedule_state.tasks_in_progress} ')

            if in_progress_jobs >= schedule_state.available_slots:
                logger.info('IN PROGRESS jobs is more than LIMIT , So No Available Slots in FOR TJW')
                schedule_state.available_slots = 0
                logger.info(
                    f'SCHEDULE STATE UPDATED FOR TJW :- \n available_slots : '
                    f'{schedule_state.available_slots} ')
                return schedule_state
            else:
                schedule_state.available_slots = schedule_state.available_slots - in_progress_jobs
                logger.info(
                    f'SCHEDULE STATE UPDATED FOR TJW :- \n available_slots : '
                    f'{schedule_state.available_slots} ')

        # Check other jobs
        job_list = list(filter(lambda x: x.status == JobStatus.Ready_To_Schedule.value, tj_list))

        if len(job_list) > 0:
            logger.info('In FOR TJW READY TO SCHEDULE jobs is greater than 0')
            new_mapped_tasks = self.__map_task(job_list[0:schedule_state.available_slots])
            schedule_state.tasks = schedule_state.tasks + new_mapped_tasks
            schedule_state.available_slots = schedule_state.available_slots - len(new_mapped_tasks)
            schedule_state.create_cluster = True
            logger.info(
                f'FOR TJW SCHEDULE STATE UPDATED :- \n create_cluster : {schedule_state.create_cluster} '
                f'\n  tasks : {schedule_state.tasks}'
                f'\n  available_slots : {schedule_state.available_slots}')
            if schedule_state.available_slots == 0:
                return schedule_state

        job_list = list(filter(lambda x: x.status == JobStatus.Processed.value, tj_list))
        job_list = list(filter(lambda x: x.is_dependent == False, job_list))

        logger.info(f'PROCESSED JOBS IS DEPENDENT FALSE {job_list}')
        if len(job_list) > 0:
            logger.info('FOR TJW ,PROCESSED jobs is greater than 0')
            new_mapped_tasks = self.__map_task(job_list[0:schedule_state.available_slots])
            schedule_state.tasks = schedule_state.tasks + new_mapped_tasks
            schedule_state.create_cluster = True
            schedule_state.available_slots = schedule_state.available_slots - len(new_mapped_tasks)
            logger.info(
                f'FOR TJW SCHEDULE STATE UPDATED :- \n create_cluster : {schedule_state.create_cluster} '
                f'\n  tasks : {schedule_state.tasks}'
                f'\n  available_slots : {schedule_state.available_slots}')
            if schedule_state.available_slots == 0:
                return schedule_state

        job_list = list(filter(lambda x: x.status == JobStatus.Created.value, tj_list))
        job_list = list(filter(lambda x: x.is_dependent == False, job_list))

        logger.info(f'CREATED JOBS IS DEPENDENT FALSE {job_list}')
        if len(job_list) > 0:
            logger.info('FOR TJW ,CREATED jobs is greater than 0')
            new_mapped_tasks = self.__map_task(job_list[0:schedule_state.available_slots])
            schedule_state.tasks = schedule_state.tasks + new_mapped_tasks
            schedule_state.create_cluster = True
            schedule_state.available_slots = schedule_state.available_slots - len(new_mapped_tasks)
            logger.info(
                f'FOR TJW SCHEDULE STATE UPDATED :- \n create_cluster : '
                f'{schedule_state.create_cluster} \n '
                f' tasks : {schedule_state.tasks}'
                f'\n  available_slots : {schedule_state.available_slots}')

            if schedule_state.available_slots == 0:
                return schedule_state

        job_list = list(filter(lambda x: x.status == JobStatus.Failed.value, tj_list))
        job_list = list(filter(lambda x: x.is_dependent == False, job_list))

        logger.info(f'FAILED JOBS IS DEPENDENT FALSE {job_list}')
        if len(job_list) > 0:
            logger.info('FOR TJW ,FAILED jobs is greater than 0')
            new_mapped_tasks = self.__map_task(job_list[0:schedule_state.available_slots])
            schedule_state.tasks = schedule_state.tasks + new_mapped_tasks
            schedule_state.create_cluster = True
            schedule_state.available_slots = schedule_state.available_slots - len(new_mapped_tasks)
            logger.info(
                f'FOR TJW SCHEDULE STATE UPDATED :- \n create_cluster : {schedule_state.create_cluster} '
                f'\n  tasks : {schedule_state.tasks}'
                f'\n  available_slots : {schedule_state.available_slots}')

        return schedule_state

    def __map_task(self, tasks):
        result = []

        for item in tasks:
            task = Task(item.model, TaskType.Transform.name, item.process_config, item.id, "", item.job_type,
                        str(item.last_success_date))
            result.append(task.__dict__)
        return result

    def __time_range_jobs(self, tasks):
        temp_tasks = tasks.copy()

        for item in tasks:
            print(item.process_config)
            if item.frequency["schedule_interval"] == JobInterval.Monthly.value:
                first = utc_now().replace(tzinfo=pytz.utc)
                second = item.last_success_date.replace(tzinfo=pytz.utc)
                if date_diffrence_days(first, second) < Settings.Schedule["month_gap"]:
                    temp_tasks.remove(item)

            elif item.frequency["schedule_interval"] == JobInterval.Weekly.value:
                first = utc_now().replace(tzinfo=pytz.utc)
                second = item.last_success_date.replace(tzinfo=pytz.utc)
                if date_diffrence_days(first, second) < Settings.Schedule["week_gap"]:
                    temp_tasks.remove(item)

            elif item.frequency["schedule_interval"] == JobInterval.Daily.value:
                first = utc_now().replace(tzinfo=pytz.utc)
                second = item.last_success_date.replace(tzinfo=pytz.utc)
                if utc_date_diff_in_hour(first, second) < Settings.Schedule["day_gap"]:
                    temp_tasks.remove(item)

            elif item.frequency["schedule_interval"] == JobInterval.Once.value:
                if item.status == JobStatus.Processed.value:
                    temp_tasks.remove(item)
        return temp_tasks
