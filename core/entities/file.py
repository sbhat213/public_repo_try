from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, NumberAttribute, MapAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
from pynamodb.models import Model

from core.entities.entity import EntityMeta
from core.entities.file_status import FileStatus


class FileStatusIndex(GlobalSecondaryIndex):
    """
    This class represents a process secondary index
    """
    created_month = UnicodeAttribute(hash_key=True)
    status = NumberAttribute(range_key=True)

    class Meta(EntityMeta):
        read_capacity_units = 1
        write_capacity_units = 1
        index_name = 'status-created_on-in'
        projection = AllProjection()


class FileExecution(MapAttribute):
    response_id = NumberAttribute(null=True)
    error = UnicodeAttribute(null=True)
    error_code = UnicodeAttribute(null=True)
    livy_url = UnicodeAttribute(null=True)
    batch_id = UnicodeAttribute(null=True)
    running_duration = NumberAttribute(default=0)


class File(Model):
    id = UnicodeAttribute(hash_key=True, null=False)
    name = UnicodeAttribute(null=False)
    model = UnicodeAttribute(null=False)
    url = UnicodeAttribute(null=False)
    created_month = UnicodeAttribute()
    created_on = UTCDateTimeAttribute()
    updated_on = UTCDateTimeAttribute()
    status = NumberAttribute(default=FileStatus.File_Submitted.value)
    retry_count = NumberAttribute(default=0)
    created_by = UnicodeAttribute(null=False)
    updated_by = UnicodeAttribute(null=False)
    file_status_index = FileStatusIndex()
    last_success_date = UTCDateTimeAttribute(null=True)
    file_execution_details = FileExecution(null=True)

    class Meta(EntityMeta):
        table_name = 'file'
        abstract = False
