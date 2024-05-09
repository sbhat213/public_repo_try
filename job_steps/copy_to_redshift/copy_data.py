from injector import inject
from aws_cloud.storage.file_storage import FileStorage
from shared.redshift.redshift_sql_client import RedshiftSQLClient
from settings import Settings


class CopyData:

    @inject
    def __init__(self, file_storage: FileStorage, redshift_sql_client: RedshiftSQLClient,
                 settings: Settings):
        self.file_storage = file_storage
        self.redshift_sql_client = redshift_sql_client
        self.settings = settings
        self.aws_access_key = settings.AWS_CREDENTIALS['access_key_id']
        self.aws_secret_key = settings.AWS_CREDENTIALS['secret_access_key']
        self.skip_list = ['gz', 'GZ']

    def copy(self, model, process_config):
        bucket = self.settings.s3_staging_files["bucket"]
        prefix = self.settings.s3_staging_files["staging_prefix"].format(model.name)
        processed_prefix = self.settings.s3_staging_files["processed_prefix"].format(model.name)
        directories = self.file_storage.list_object(bucket, prefix + "/")
        sorted(directories, key=lambda i: i['Prefix'])

        for directory in directories:
            dir_prefix = directory["Prefix"]
            files = self.file_storage.list_files(bucket, dir_prefix)
            self.file_storage.delete_files(bucket, files, self.skip_list)
            for step_config in process_config.steps:
                is_truncate = step_config['params']['flag']
                print("Truncate flag : ", is_truncate)
                if (is_truncate == 'true'):

                    self.__run_overwrite(model, step_config, f"s3://{bucket}/{dir_prefix}")
                else:
                    self.__run_copy(model, f"s3://{bucket}/{dir_prefix}")

            # clear metadata files

            # Move files to processed directory
            self.file_storage.move_files(bucket, prefix, processed_prefix, files, self.skip_list)

            # update the state

    def __run_copy(self, model, path):
        redshift_table = model.table_name
        temp_table = model.table_name + "_staging"
        temp_query = f"create temp table {temp_table} (like {redshift_table}); "
        copy_query = f"copy {temp_table} from '{path}' IGNOREHEADER 1 delimiter ',' gzip  credentials 'aws_access_key_id={self.aws_access_key};aws_secret_access_key={self.aws_secret_key}' CSV;"
        update_query = f"begin transaction;" \
                       f"delete from {redshift_table} using {temp_table} " \
                       f"where {redshift_table}.{model.primary_key} = {temp_table}.{model.primary_key}; " \
                       f"insert into {redshift_table} " \
                       f"select * from {temp_table};" \
                       f"end transaction;" \
                       f"drop table {temp_table};"
        query = f"{temp_query}{copy_query}{update_query}"
        print(query)
        self.redshift_sql_client.execute_query(query)

    def __run_overwrite(self, model, step_config, path):

        redshift_table = model.table_name
        temp_table = model.table_name + "_staging"
        condition_list = step_config['params']['condition']
        print('condition_list', (condition_list))

        # Checking for where condition
        # if true - delete data from redshift table on where condition and insert,
        # if false - truncate the redshift table and insert.
        if (condition_list):
            where_query = f"where"
            for i, config in enumerate(condition_list):
                print(i, config['name'])
                if (i == len(condition_list) - 1):
                    where_query += f"({redshift_table}.{config['name']} = '{config['value']}');"
                else:
                    where_query += f"({redshift_table}.{config['name']} = '{config['value']}') and "

            temp_query = f"create temp table {temp_table} (like {redshift_table}); "
            copy_query = f"copy {temp_table} from '{path}' IGNOREHEADER 1 delimiter ',' gzip  credentials 'aws_access_key_id={self.aws_access_key};aws_secret_access_key={self.aws_secret_key}' CSV;"
            update_query = f"begin transaction;" \
                           f"delete from {redshift_table} " \
                           f"{where_query}" \
                           f"delete from {redshift_table} using {temp_table} " \
                           f"where {redshift_table}.{model.primary_key} = {temp_table}.{model.primary_key}; " \
                           f"insert into {redshift_table} " \
                           f"select * from {temp_table};" \
                           f"end transaction;" \
                           f"drop table {temp_table};"
            query = f"{temp_query}{copy_query}{update_query}"
            print(query)

        else:
            copy_query = f"copy {redshift_table} from '{path}' IGNOREHEADER 1 delimiter ',' gzip  credentials 'aws_access_key_id={self.aws_access_key};aws_secret_access_key={self.aws_secret_key}' CSV;"
            update_query = f"begin transaction;" \
                           f"truncate {redshift_table};" \
                           f"end transaction;"
            query = f"{update_query}{copy_query}"
            print(query)

        self.redshift_sql_client.execute_query(query)
