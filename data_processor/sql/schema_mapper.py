from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType


class SchemaMapper:

    def get_sql_map(self, model, left_table, right_table):
        fields = []
        for field in model.fields:
            param = "{}.{}={}.{}".format(left_table, field.name, right_table, field.name)
            fields.append(param)
        return " ,".join(fields)

    def get_model_schema(self, model):
        fields = []
        for field in model.fields:
            nullable = True
            if field.name == model.primary_key:
                nullable = False
            struct = StructField(field.name, self.get_field_type(field.type), nullable=nullable)

            fields.append(struct)
        schema = StructType(fields)
        return schema

    def get_field_type(self, name):
        if name is None or name == 'varchar' or name == 'char':
            return StringType()
        elif name == 'datetime':
            return TimestampType()
        elif name == 'bigint':
            return IntegerType()
        else:
            return StringType()
