from pynamodb.constants import PAY_PER_REQUEST_BILLING_MODE
from utilities.migration import  injector

from aws_cloud.database.no_sql_db import NoSQLDB
from core.entities.file_model import FileModel


class FileModelMigrator:

    def migrate_schema(self, truncate=False):
        db = injector.get(NoSQLDB)

        if truncate:
            db.truncate_table(FileModel)
        else:
            db.create_table(FileModel, PAY_PER_REQUEST_BILLING_MODE, True)
