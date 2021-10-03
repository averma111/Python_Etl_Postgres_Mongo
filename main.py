# Importing internal dependenciesS
from dbcredentials import databases
from etl import performetl

# Pipeline constants
PRINT_INFO = True
PRINT_RESULTS = True


def main():
    if PRINT_INFO:
        print('Starting the data pipeline')
        database = databases.Database()
        print('Initializing the postgres connection')
    postgres = database.initialize_postgresql()
        

    if PRINT_INFO:
        etl = performetl.Performetl()
        print('Postgres connection completed')
        print('Starting the data stage 1: Extracting data from postgresql')
        postgres_cur = postgres.cursor()
    postgres_data = etl.extarct_postgres_data(postgres_cur)
    

    if PRINT_INFO:
        print('Stage 1 completed! Data successfully extracted from MySQL')
        print('Starting data pipeline stage 2: Transforming data from MySQL for MongoDB')
        print('Transforming genres dataset')
    employee_collection = etl.transform_data(list(postgres_data),'employee')

    if PRINT_INFO:
        print('Starting connection to mongodb')
        database = databases.Database()
    mongo = database.initialize_mongodb()

    if PRINT_INFO:
        print('MongoDB connection Completed')
        print('Starting data pipeline stage 3: Loading data into MongoDB')
    result = etl.load_mongo_data(mongo['employee'], employee_collection)

    if PRINT_RESULTS:
        print('Successfully loaded employees')
        print(result)
    
    if PRINT_INFO:
        print('Stage 3 completed! Data successfully loaded')
        print('Closing Postgres connection')
    postgres.close()
    if PRINT_INFO:
        print('Postgresql connection closed successfully')
        print('Ending data pipeline')



if __name__ == '__main__':
    main()

