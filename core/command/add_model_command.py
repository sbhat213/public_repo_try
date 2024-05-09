class AddModelCommand:
    def __init__(self, name: str, path: str, fields: str, primary_key: str, composite_key: list, partition_key: str,
                 table_name: str, created_by: str, updated_by: str):
        self.name = name
        self.path = path
        self.fields = fields
        self.primary_key = primary_key
        self.composite_key = composite_key
        self.partition_key = partition_key
        self.table_name = table_name
        self.created_by = created_by
        self.updated_by = updated_by
