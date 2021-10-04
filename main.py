# Importing internal dependenciesS
from dbcredentials import databases
from etl import performetl
import logging
import logging.config

# Pipeline constants
PRINT_INFO = True
PRINT_RESULTS = True

logging.config.fileConfig(fname='logs/logs.ini', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


# logger = logging.getLogger()
# logging.basicConfig(filename="etl.logs", format='%(filename)s: %(asctime)s %(message)s', filemode='w')
# logger.setLevel(logging.INFO)


def main():
    if PRINT_INFO:
        logger.info('Starting the data pipeline')
        database = databases.Database()
        logger.info('Initializing the postgres connection')
    postgres = database.initialize_postgresql()

    if PRINT_INFO:
        etl = performetl.Performetl()
        logger.info('Postgres connection completed')
        logger.info('Starting the data stage 1: Extracting data from postgresql')
        postgres_cur = postgres.cursor()
    postgres_data = etl.extarct_postgres_data(postgres_cur)

    if PRINT_INFO:
        logger.info('Stage 1 completed! Data successfully extracted from MySQL')
        logger.info('Starting data pipeline stage 2: Transforming data from MySQL for MongoDB')
        logger.info('Transforming genres dataset')
    employee_collection = etl.transform_data(list(postgres_data), 'employee')

    if PRINT_INFO:
        logger.info('Starting connection to mongodb')
        database = databases.Database()
    mongo = database.initialize_mongodb()

    if PRINT_INFO:
        logger.info('MongoDB connection Completed')
        logger.info('Starting data pipeline stage 3: Loading data into MongoDB')
    result = etl.load_mongo_data(mongo['employee'], employee_collection)

    if PRINT_RESULTS:
        logger.info('Successfully loaded employees')

    if PRINT_INFO:
        logger.info('Stage 3 completed! Data successfully loaded')
        logger.info('Closing Postgres connection')
    postgres.close()
    if PRINT_INFO:
        logger.info('Postgresql connection closed successfully')
        logger.info('Ending data pipeline')


if __name__ == '__main__':
    main()