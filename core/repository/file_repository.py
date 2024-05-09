from core.entities.file import File
from pynamodb.attributes import UTCDateTimeAttribute
from shared.utils.datetime import utc_now, year_month, previous_year_month
from datetime import timedelta


class FileRepository:

    def add(self, file: File):
        file.save()

    def update_status(self, file: File):
        file.update(
            actions=[
                File.status.set(file.status)
            ]
        )

    def get(self, file_id):
        file = list(File.query(hash_key=file_id))[0]
        return file

    def get_files(self, files):
        files = File.batch_get(files)
        return list(files)

    def get_files_by_status(self, current_month, previous_month, is_previous_flag, created, status, limit=0,
                            retry_count=0):
        if is_previous_flag:
            files_current = File.file_status_index.query(previous_month, File.status == status,
                                                         filter_condition=(File.retry_count < retry_count)
                                                                          & (File.created_on >= created), limit=limit)

            files_previous = File.file_status_index.query(current_month, File.status == status,
                                                          filter_condition=(File.retry_count < retry_count)
                                                                           & (File.created_on >= created), limit=limit)
            files = list(files_current) + list(files_previous)
            return files

        files = File.file_status_index.query(current_month, File.status == status,
                                             filter_condition=(File.retry_count < retry_count)
                                                              & (File.created_on >= created), limit=limit)

        return list(files)
