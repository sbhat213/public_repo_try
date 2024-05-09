from datetime import timedelta
from shared.utils.datetime import utc_now, year_month, previous_year_month, is_previous_range,date_to_str
from injector import inject
from job_steps import injector
from shared.logging.logger import Logger
from core.entities.file_status import FileStatus
from core.repository.file_repository import FileRepository
from settings import Settings
from job_steps.dto.check_spark_state import CheckSparkState
from job_steps.dto.task_type import TaskType
from core.entities.job_type import JobType


class InProgressFileWatcher:
    @inject
    def __init__(self, file_repository: FileRepository, settings: Settings):
        self.settings = settings
        self.file_repository = file_repository
        self.limit = settings.Schedule["check_limit"]
        self.retry_count = settings.Schedule["retry_count"]
        self.interval_days = settings.Schedule["interval_days"]

    def get_tasks(self, schedule_state):

        date = utc_now() - timedelta(days=self.interval_days)
        current_month = year_month()
        previous_month = previous_year_month()

        return self.__get_monthly_task(schedule_state, current_month, previous_month, date,
                                       is_previous_range(self.interval_days))

    def __get_monthly_task(self, schedule_state, current_month, previous_month, date, is_previous_flag):
        logger = injector.get(Logger)
        # Check in-progress files
        file_list = self.file_repository.get_files_by_status(current_month, previous_month, is_previous_flag, date,
                                                             FileStatus.In_Progress.value,
                                                             self.retry_count,
                                                             self.limit)

        in_progress_files = len(file_list)
        logger.info(
            f"INPROGRESS FILE WATCHER STATUS : \n Length of file_list IN PROGRESS : {in_progress_files} , Limit : {self.limit} \n List of Files In Progress : {file_list} ")
        schedule_state.available_slots = self.limit
        logger.info(f"SCHEDULE STATE UPDATED :- \n available_slots :  {schedule_state.available_slots}")
        if in_progress_files > 0:
            logger.info("In Progress Files are Greater Than Zero")
            print(self.limit, in_progress_files)
            if in_progress_files <= self.limit:
                logger.info("In Progress Files are Greater/Equal to Limit")
                schedule_state.css_tasks = self.__map_css_task(file_list[0:self.limit - in_progress_files])

                schedule_state.available_slots = self.limit - len(schedule_state.css_tasks)
                logger.info(
                    f'SCHEDULE STATE UPDATED :-  \n available_slots : {schedule_state.available_slots} \n tasks : {schedule_state.css_tasks}')
            return schedule_state

    def __map_css_task(self, tasks):
        result = []
        for item in tasks:
            job_type = JobType.Spark.value
            css_task = CheckSparkState(item.model, item.file_execution_details.response_id,
                                       item.file_execution_details.livy_url,
                                       item.id, TaskType.File.name, job_type,
                                       date_to_str(item.updated_on))

            result.append(css_task.__dict__)

        return result
