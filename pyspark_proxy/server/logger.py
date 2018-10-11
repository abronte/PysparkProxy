import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

LOG_LEVELS = {
    'CRITICAL': logging.CRITICAL,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG
    }

LOG_PATH = '/var/log/pyspark_proxy_server'

logger = None

def configure_logging(stdout, level):
    global logger
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')

    logger = logging.getLogger()
    logger.setLevel(LOG_LEVELS[level])

    if stdout:
        handler = logging.StreamHandler(stream=sys.stdout)
    else:
        if not os.path.exists(LOG_PATH):
            os.makedirs(LOG_PATH)

        handler = TimedRotatingFileHandler(
               LOG_PATH+'/server.log',
               when='midnight',
               interval=1,
               backupCount=7)

    handler.setFormatter(formatter)
    logger.addHandler(handler)
