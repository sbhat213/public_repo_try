from utilities.migration import  injector
from pynamodb.constants import PAY_PER_REQUEST_BILLING_MODE

from aws_cloud.database.no_sql_db import NoSQLDB
from core.entities.process_configuration import ProcessConfiguration
from core.entities.transformation_job import TransformationJob

class ConfigMigrator:

    def migrate_schema(self, truncate=False):
        db = injector.get(NoSQLDB)

        if truncate:
            db.truncate_table(ProcessConfiguration)
        else:
            db.create_table(ProcessConfiguration, PAY_PER_REQUEST_BILLING_MODE, True)
            db.create_table(TransformationJob, PAY_PER_REQUEST_BILLING_MODE, True)

