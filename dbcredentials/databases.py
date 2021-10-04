# Importing the packages
from pymongo import MongoClient
import psycopg2
import configparser
import sys
import logging.config

config = configparser.ConfigParser()
config.read('config/config.ini')

logging.config.fileConfig(fname='logs/logs.ini', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class Database:

    def initialize_postgresql(self):
        try:
            return psycopg2.connect(
                user=config['postgresql']['PG_USERNAME'],
                password=config['postgresql']['PG_PASSWORD'],
                host=config['postgresql']['PG_HOSTNAME'],
                port=config['postgresql']['PG_PORT'],
                database=config['postgresql']['PG_DATABASE']
            )
        except psycopg2.OperationalError as err:
            logger.error('Error connecting to postgresql database')
            print_psycopg2_exception(err)

    def initialize_mongodb(self):
        return MongoClient(config['mongodb']['MONGO_HOST'],
                           int(config['mongodb']['MONGO_PORT']))[config['mongodb']['MONGO_DB']]


def print_psycopg2_exception(err):
    err_type, err_obj, traceback = sys.exc_info()
    line_num = traceback.tb_lineno
    logger.error("\npsycopg2 ERROR:", err, "on line number:", line_num)
    logger.error("psycopg2 traceback:", traceback, "-- type:", err_type)
    logger.error("\nextensions.Diagnostics:", err.diag)
    logger.error("pgerror:", err.pgerror)
    logger.error("pgcode:", err.pgcode, "\n")