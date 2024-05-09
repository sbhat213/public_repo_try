from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute
from core.entities.entity import EntityMeta
from pynamodb.models import Model


class FileModel(Model):
    file = UnicodeAttribute(hash_key=True, null=False)
    model = UnicodeAttribute(null=False)
    created_on = UTCDateTimeAttribute(null=False)
    updated_on = UTCDateTimeAttribute(null=False)

    class Meta(EntityMeta):
        table_name = 'file_model'
        abstract = False
