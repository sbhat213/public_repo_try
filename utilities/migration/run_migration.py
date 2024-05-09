from utilities.migration.dynamodb.file_migrator import FileMigrator
from utilities.migration.dynamodb.model_migrator import ModelMigrator
from utilities.migration.dynamodb.config_migrator import ConfigMigrator
from utilities.migration.dynamodb.file_model_migrator import FileModelMigrator

from utilities.migration.redshift.table_migrator import TableMigrator
from utilities.migration import injector

if __name__ == '__main__':
    # fm = FileMigrator()
    # fm.migrate_schema()
    #
    # mm = ModelMigrator()
    # mm.migrate_schema()
    #
    # cm = ConfigMigrator()
    # cm.migrate_schema()
    #
    # fmm = FileModelMigrator()
    # fmm.migrate_schema()

    tm = injector.get(TableMigrator)
    tm.migrate_schema()
