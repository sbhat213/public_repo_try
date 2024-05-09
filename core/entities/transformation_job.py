from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, NumberAttribute, BooleanAttribute, MapAttribute, \
    ListAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
from pynamodb.models import Model
from core.entities.entity import EntityMeta
from core.entities.job_status import JobStatus


class Frequency(MapAttribute):
    schedule_interval = UnicodeAttribute(null=True)
    execution_time = UnicodeAttribute(null=True)
    execution_date = UnicodeAttribute(null=True)


class JobExecution(MapAttribute):
    response_id = NumberAttribute(null=True)
    error = UnicodeAttribute(null=True)
    error_code = UnicodeAttribute(null=True)
    livy_url = UnicodeAttribute(null=True)
    batch_id = UnicodeAttribute(null=True)
    running_duration = NumberAttribute(default=0)


class TransformationJob(Model):
    id = UnicodeAttribute(hash_key=True, null=False)
    model = UnicodeAttribute(null=False)
    process_config = UnicodeAttribute(null=False)
    active = BooleanAttribute(null=False)
    status = NumberAttribute(default=JobStatus.Created.value)
    is_dependent = BooleanAttribute(null=False)
    dependent_jobs = ListAttribute(null=True)
    frequency = Frequency()
    created_on = UTCDateTimeAttribute(null=False)
    updated_on = UTCDateTimeAttribute(null=True)
    created_by = UnicodeAttribute(null=True)
    updated_by = UnicodeAttribute(null=True)
    last_success_date = UTCDateTimeAttribute(null=True)
    job_type = NumberAttribute(null=False)
    retry_count = NumberAttribute(null=True)
    last_execution_detail = JobExecution(null=True)

    class Meta(EntityMeta):
        table_name = 'transformation_job'
        abstract = False
