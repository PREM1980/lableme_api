from os.path import dirname, abspath
import logging
from logging.config import fileConfig

d = dirname(dirname(abspath(__file__)))
config_file = d+'/log/logging_config.ini'
fileConfig(config_file)
logger = logging.getLogger()
