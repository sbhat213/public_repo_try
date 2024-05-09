import psycopg2
from psycopg2 import pool
from settings import Settings
from shared.logging.logger import Logger


class RedshiftSQLClient:

    def __init__(self, redshift_credentials, logger):
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(1, 4, user=redshift_credentials["user_name"],
                                                                  password=redshift_credentials["password"],
                                                                  host=redshift_credentials["host"],
                                                                  port=redshift_credentials["port"],
                                                                  database=redshift_credentials["database"])
        self.logger = logger

    def execute_query(self, query):
        logger = self.logger
        connection = self.connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(query)
        cursor.close()
        connection.commit()
        self.connection_pool.putconn(connection)
