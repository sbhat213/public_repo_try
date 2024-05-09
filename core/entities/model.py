from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, MapAttribute, ListAttribute, BooleanAttribute, \
    NumberAttribute
from core.entities.entity import EntityMeta
from pynamodb.models import Model


class Fields(MapAttribute):
    name = UnicodeAttribute(null=False)
    data_type = UnicodeAttribute(null=False)
    length = NumberAttribute(null=False)
    core_field = BooleanAttribute(null=False)


class Model(Model):
    name = UnicodeAttribute(hash_key=True, null=False)
    path = UnicodeAttribute(null=False)
    fields = ListAttribute(of=Fields)
    primary_key = UnicodeAttribute(null=False)
    composite_key = ListAttribute(null=True)
    partition_key = UnicodeAttribute(null=True)
    table_name = UnicodeAttribute(null=False)
    created_on = UTCDateTimeAttribute(null=False)
    updated_on = UTCDateTimeAttribute(null=False)
    created_by = UnicodeAttribute(null=False)
    updated_by = UnicodeAttribute(null=False)

    class Meta(EntityMeta):
        table_name = 'model'
        abstract = False
