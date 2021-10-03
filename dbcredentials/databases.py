# Importing the packages
from pymongo import MongoClient
import psycopg2
import configparser
import sys
import logging

config = configparser.ConfigParser()
config.read('config/config.ini')

logger = logging.getLogger()
logging.basicConfig(filename="logs/etl.log", format='%(filename)s: %(asctime)s %(message)s', filemode='w')
logger.setLevel(logging.INFO)

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
            logging.error('Error connecting to postgresql database')
            print_psycopg2_exception(err)
            return None

    def initialize_mongodb(self):
        return MongoClient(config['mongodb']['MONGO_HOST'],
                           int(config['mongodb']['MONGO_PORT']))[config['mongodb']['MONGO_DB']]


def print_psycopg2_exception(err):
    err_type, err_obj, traceback = sys.exc_info()
    line_num = traceback.tb_lineno
    logging.error("\npsycopg2 ERROR:", err, "on line number:", line_num)
    logging.error("psycopg2 traceback:", traceback, "-- type:", err_type)
    logging.error("\nextensions.Diagnostics:", err.diag)
    logging.error("pgerror:", err.pgerror)
    logging.error("pgcode:", err.pgcode, "\n")