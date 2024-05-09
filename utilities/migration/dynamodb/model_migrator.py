from pynamodb.constants import PAY_PER_REQUEST_BILLING_MODE
from utilities.migration import injector

from aws_cloud.database.no_sql_db import NoSQLDB
from core.entities.model import Model


class ModelMigrator:

    def migrate_schema(self, truncate=False):
        db = injector.get(NoSQLDB)

        if truncate:
            db.truncate_table(Model)
        else:
            db.create_table(Model, PAY_PER_REQUEST_BILLING_MODE, True)
