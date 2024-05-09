import logging
import os


class FileLogger(object):

    def __init__(self):
        logger = logging.getLogger(__name__)  # log_namespace can be replaced with your namespace
        logger.setLevel("INFO")
        self._logger = logger

    def info(self, message):
        self._logger.info(message)

    def error(self, message):
        self._logger.error(message)

    def debug(self, message):
        self._logger.debug(message)

    def warning(self, message):
        self._logger.warning(message)
