import os
import pandas as pd
import pandas_gbq as pd_gbq
from sqlalchemy import create_engine
from google.oauth2 import service_account

# Json Google Cloud credentials
credentials = service_account.Credentials.from_service_account_file('credentials.json')

# BigQuery credentials
PROJECT_ID = 'your_project_id'
DATASET_ID = 'your_dataset_id'

# PostgreSQL
PSQL_USERNAME = 'postgres'
PSQL_PASSWORD = 'your_password'
PSQL_HOST = 'ec9–9–99–9–99.compute-9.amazonaws.com'
PSQL_DATABASE = 'test_db'

# Path for the dump directory
DIRECTORY = 'dump'
def main():

    #Let's begin!
    print("hola py")
    
    con_uri = "postgresql://{}:{}@{}/{}" .format(
        PSQL_USERNAME,
        PSQL_PASSWORD,
        PSQL_HOST,
        PSQL_DATABASE
    )

    print("PSQL url {}".format(con_uri))
    
    try:
        engine = create_engine(con_uri, pool_recycle=3600).connect()
    except Exception as e:
        print("Error {}".format(e))
    
    tables_query = "SELECT table_name " \
                   "FROM information_schema.tables " \
                   "WHERE TABLE_SCHEMA = 'public';" \
    
    list_tables = pd.read_sql(tables_query, con_uri)

    #This print is only for information
    print(list_tables)

    #Iterate over table list, get data and upload to BigQuery via pandas_gbq 
    for index, row in list_tables.iterrows():

        table_id = '{}.{}'.format(DATASET_ID, row['table_name'])

        print("Loading Table {}".format(table_id)
        df = pd.read_sql_table(row['table_name'], engine)

        pd_gbq.to_gbq(df, table_id,
                      project_id=PROJECT_ID,
                      if_exists='replace',
                      chunksize=1000000
                      progress_bar=True)

if __name__ == '__main__':
    main()