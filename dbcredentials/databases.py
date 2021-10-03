# Importing the packages
from pymongo import MongoClient
import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('config/config.ini')

class Database:

    def initialize_postgresql(self):
        return psycopg2.connect(
            user=config['postgresql']['PG_USERNAME'],
            password=config['postgresql']['PG_PASSWORD'],
            host=config['postgresql']['PG_HOSTNAME'],
            port=config['postgresql']['PG_PORT'],
            database=config['postgresql']['PG_DATABASE']
        )

    def initialize_mongodb(self):
        return MongoClient(config['mongodb']['MONGO_HOST'],
                           int(config['mongodb']['MONGO_PORT']))[config['mongodb']['MONGO_DB']]
