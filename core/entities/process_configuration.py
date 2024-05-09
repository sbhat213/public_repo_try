from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, ListAttribute, MapAttribute

from core.entities.entity import EntityMeta
from pynamodb.models import Model


class ProcessStep(MapAttribute):
    name = UnicodeAttribute(null=False)
    depends_on = UnicodeAttribute(null=True)
    params = MapAttribute(null=True)


class ProcessConfiguration(Model):
    name = UnicodeAttribute(hash_key=True, null=False)
    steps = ListAttribute(of=ProcessStep, null=False)
    depends_on = ListAttribute(null=True)
    global_map = MapAttribute(null=True)
    created_on = UTCDateTimeAttribute(null=False)
    updated_on = UTCDateTimeAttribute(null=False)

    class Meta(EntityMeta):
        table_name = 'process_configuration'
        abstract = False
