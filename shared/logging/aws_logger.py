import logging

import boto3
import watchtower


class AWSLogger():
    __instance = None

    @staticmethod
    def getInstance(settings, app_name, send_interval=0):
        """ Static access method. """
        if AWSLogger.__instance == None:
            AWSLogger(settings, app_name, send_interval)
        return AWSLogger.__instance

    def __init__(self, settings, app_name, send_interval):
        """ Virtually private constructor. """
        if AWSLogger.__instance != None:
            return AWSLogger.__instance
        else:
            AWSLogger.__instance = self

        super(AWSLogger, self).__init__()

        if settings is None:
            session = None
        else:
            session = boto3.session.Session(aws_access_key_id=settings["access_key_id"],
                                            aws_secret_access_key=settings["secret_access_key"],
                                            region_name=settings['region'])
        logging.basicConfig()
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        # self.logger.setFormatter(logging.Formatter())

        self.logger.addHandler(
            watchtower.CloudWatchLogHandler(stream_name=app_name, boto3_session=session, send_interval=send_interval))
        self.context = ""

    def _join(self, strings):
        return ' '.join(filter(None, strings))

    def debug(self, message, resource_id=''):
        self.logger.debug(self._join([logging.getLevelName(logging.DEBUG), resource_id, self.context, str(message)]))

    def info(self, message, resource_id=''):
        self.logger.info(self._join([logging.getLevelName(logging.INFO), resource_id, self.context, str(message)]))

    def warning(self, message, resource_id='', context=''):
        self.logger.warning(self._join([logging.getLevelName(logging.WARNING),
                                        resource_id, self.context, str(message)]))

    def error(self, message, resource_id=''):
        self.logger.error(self._join([logging.getLevelName(logging.ERROR), resource_id, self.context, str(message)]))

    def fatal(self, message, resource_id=''):
        self.logger.fatal(self._join([logging.getLevelName(logging.FATAL), resource_id, self.context, str(message)]))
