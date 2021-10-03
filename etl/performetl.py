# Importing the dependencies
import copy

# Pipeline constants
RESET_MONGO_COLLECTIONS_ON_UPDATE = True


class Performetl:

    def extarct_postgres_data(self, pg_cursor):
        pg_cursor.execute(
            """select id,first_name,last_name from employees.employee""")
        table_employee = pg_cursor.fetchall()
        return table_employee

    def transform_data(self, dataset, table):
        dataset_collection = []
        temp_collection = {}

        if table == 'employee':
            for item in dataset:
                temp_collection['id'] = item[0]
                temp_collection['first_name'] = item[1]
                temp_collection['last_name'] = item[2]
                dataset_collection.append(copy.copy(temp_collection))

            return dataset_collection

    def load_mongo_data(self, mongo_collection, dataset_collection):
        if RESET_MONGO_COLLECTIONS_ON_UPDATE:
            mongo_collection.delete_many({})

        return mongo_collection.insert_many(dataset_collection)
