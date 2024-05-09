from types import SimpleNamespace


class Model:
    def __init__(self, data):
        obj = SimpleNamespace(**data)
        self.name = obj.name
        self.path = obj.path
        self.primary_key = obj.primary_key
        self.composite_key = obj.composite_key
        self.partition_key = ""
        if "partition_key" in data:
            self.partition_key = obj.partition_key
        self.table_name = obj.table_name
        values = []
        for item in obj.fields:
            values.append(Field(item))
        self.fields = values


class Field:
    def __init__(self, data):
        obj = SimpleNamespace(**data)
        self.name = obj.name
        self.type = obj.data_type
        self.length = obj.length
        self.core_field = obj.core_field
