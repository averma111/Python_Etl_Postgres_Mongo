# Importing internal dependenciesS
from dbcredentials import databases
from etl import performetl
import logging

# Pipeline constants
PRINT_INFO = True
PRINT_RESULTS = True

logger = logging.getLogger()
logging.basicConfig(filename="logs/etl.log", format='%(filename)s: %(asctime)s %(message)s', filemode='w')
logger.setLevel(logging.INFO)

def main():
    if PRINT_INFO:
        logging.info('Starting the data pipeline')
        database = databases.Database()
        logging.info('Initializing the postgres connection')
    postgres = database.initialize_postgresql()
        

    if PRINT_INFO:
        etl = performetl.Performetl()
        logging.info('Postgres connection completed')
        logging.info('Starting the data stage 1: Extracting data from postgresql')
        postgres_cur = postgres.cursor()
    postgres_data = etl.extarct_postgres_data(postgres_cur)
    

    if PRINT_INFO:
        logging.info('Stage 1 completed! Data successfully extracted from MySQL')
        logging.info('Starting data pipeline stage 2: Transforming data from MySQL for MongoDB')
        logging.info('Transforming genres dataset')
    employee_collection = etl.transform_data(list(postgres_data),'employee')

    if PRINT_INFO:
        logging.info('Starting connection to mongodb')
        database = databases.Database()
    mongo = database.initialize_mongodb()

    if PRINT_INFO:
        logging.info('MongoDB connection Completed')
        logging.info('Starting data pipeline stage 3: Loading data into MongoDB')
    result = etl.load_mongo_data(mongo['employee'], employee_collection)

    if PRINT_RESULTS:
        logging.info('Successfully loaded employees')
        print('Etl is completed')

    if PRINT_INFO:
        logging.info('Stage 3 completed! Data successfully loaded')
        logging.info('Closing Postgres connection')
    postgres.close()
    if PRINT_INFO:
        logging.info('Postgresql connection closed successfully')
        logging.info('Ending data pipeline')


if __name__ == '__main__':
    main()

