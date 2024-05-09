from datetime import datetime, timedelta
from shared.utils.datetime import utc_now, year_month, previous_year_month, is_previous_range
from injector import inject
from job_steps import injector
from shared.logging.logger import Logger
from core.entities.file_status import FileStatus
from core.repository.file_repository import FileRepository
from settings import Settings
from job_steps.dto.task import Task
from job_steps.dto.task_type import TaskType
from core.entities.job_type import JobType
import os


class FileWatcher:
    @inject
    def __init__(self, file_repository: FileRepository, settings: Settings):
        self.settings = settings
        self.file_repository = file_repository
        self.limit = settings.Schedule["limit"]
        self.retry_count = settings.Schedule["retry_count"]
        self.interval_days = settings.Schedule["interval_days"]

    def get_tasks(self, schedule_state):
        logger = injector.get(Logger)
        date = utc_now() - timedelta(days=self.interval_days)
        current_month = year_month()
        previous_month = previous_year_month()
        return self.__get_monthly_task(schedule_state, current_month, previous_month, date,
                                       is_previous_range(self.interval_days))

    def __get_monthly_task(self, schedule_state, current_month, previous_month, date, is_previous_flag):
        logger = injector.get(Logger)
        # Check in-progress files
        file_list = self.file_repository.get_files_by_status(current_month, previous_month,
                                                             is_previous_flag, date,
                                                             FileStatus.In_Progress.value,
                                                             self.limit, self.retry_count)
        in_progress_files = len(file_list)
        schedule_state.available_slots = self.limit
        logger.info(
            f"FILE WATCHER STATUS : \n Length of file_list IN PROGRESS : {in_progress_files} , Limit : {self.limit}"
            f" \n List of Files In Progress : {file_list} ")

        if in_progress_files > 0:
            logger.info("In Progress Files are Greater Than Zero")
            schedule_state.create_cluster = True
            schedule_state.tasks_in_progress = True
            logger.info(
                f'SCHEDULE STATE UPDATED :- \n create_cluster : {schedule_state.create_cluster} \n '
                f'tasks_in_progress : {schedule_state.tasks_in_progress} ')
            schedule_state.available_slots = schedule_state.available_slots - in_progress_files
            if in_progress_files >= self.limit:
                logger.info("In Progress Files are Greater/Equal to Limit")
                schedule_state.available_slots = 0
                logger.info(f"SCHEDULE STATE UPDATED \n available_slots :  {schedule_state.available_slots}")
                return schedule_state

        # Check submitted files
        file_list = self.file_repository.get_files_by_status(current_month, previous_month, is_previous_flag, date,
                                                             FileStatus.File_Submitted.value,
                                                             self.limit, self.retry_count)
        submitted_files = len(file_list)
        logger.info(
            f'FILE WATCHER STATUS : \n Length of file_list SUBMITTED : {submitted_files} , Limit : {self.limit}'
            f' \n List of Files In Submitted : {file_list} ')

        if submitted_files > 0:
            logger.info("Submitted Files are Greater Than Zero")
            new_mapped_tasks = self.__map_task(file_list[0:schedule_state.available_slots])

            schedule_state.tasks = new_mapped_tasks
            schedule_state.create_cluster = True
            schedule_state.available_slots = schedule_state.available_slots - len(new_mapped_tasks)
            logger.info(
                f'SCHEDULE STATE UPDATED :- \n create_cluster : {schedule_state.create_cluster} '
                f'\n available_slots : {schedule_state.available_slots} \n tasks : {schedule_state.tasks}')
            if schedule_state.available_slots == 0:
                return schedule_state

        file_list = self.file_repository.get_files_by_status(current_month, previous_month, is_previous_flag, date,
                                                             FileStatus.Failed.value,
                                                             self.limit, self.retry_count
                                                             )
        failed_files = len(file_list)
        logger.info(
            f'FILE WATCHER STATUS : \n Length of file_list FAILED : {failed_files} , Limit : {self.limit} \n List of '
            f'Files FAILED : {file_list} ')

        if failed_files > 0:
            logger.info("Failed Files are Greater Than Zero")
            new_mapped_tasks = self.__map_task(file_list[0:schedule_state.available_slots])
            schedule_state.tasks = schedule_state.tasks + new_mapped_tasks
            schedule_state.create_cluster = True
            schedule_state.available_slots = schedule_state.available_slots - len(new_mapped_tasks)
            logger.info(
                f'SCHEDULE STATE UPDATED :- \n create_cluster : {schedule_state.create_cluster} \n available_slots '
                f': {schedule_state.available_slots} \n tasks : {schedule_state.tasks}')
            return schedule_state

    def __map_task(self, tasks):
        result = []
        for item in tasks:
            url = os.path.join(item.url, item.name)
            job_type = JobType.Spark.value
            task = Task(item.model, TaskType.File.name, item.model, item.id, url, job_type)
            result.append(task.__dict__)

        return result
