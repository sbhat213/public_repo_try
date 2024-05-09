from core.entities.transformation_job import TransformationJob
from core.entities.job_status import JobStatus


class TransformationJobRepository:

    def add(self, tranformation_job: TransformationJob):
        tranformation_job.save()

    def get(self, name):
        tranformation_job = TransformationJob.get(name)
        return tranformation_job

    def get_active_jobs(self,retry_count):
        job_list = list(TransformationJob.scan(filter_condition=(TransformationJob.active == True ) & (TransformationJob.retry_count < retry_count)))
        return job_list

    def get_inprogress_jobs(self):
        job_list = list(TransformationJob.scan(filter_condition=(TransformationJob.status == JobStatus.In_Progress.value) & (TransformationJob.active == True )))
        return job_list


