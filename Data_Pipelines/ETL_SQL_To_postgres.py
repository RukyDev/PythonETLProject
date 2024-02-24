import pandas as pd
from sqlalchemy import create_engine

# MSSQL Server connection parameters for local server
uid = 'ruky'
pwd = 'Emmanuel@68'
server = 'RUKYDEV\RUKYDEV'
database = 'AdventureWorksDW2019'
driver = 'ODBC Driver 17 for SQL Server'

# Postgress Server connection parameters for local server
puid = 'postgres'
pserver = 'localhost'
database = 'AdventureWorksDW2019'
driver = 'ODBC Driver 17 for SQL Server'

#extract data from sql server
def extract():
    try:
        print("connecting to database.......")
        # Connection string
        conn_str = f'mssql+pyodbc://{uid}:{pwd}@{server}/{database}?driver={driver}'
        # Create SQLAlchemy engine
        engine = create_engine(conn_str)
        print("connection successful")
        
        # execute query
        query = """ select  t.name as table_name
        from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales') """
        src_tables = pd.read_sql_query(query, engine).to_dict()['table_name']

        for id in src_tables:
            table_name = src_tables[id]
            df = pd.read_sql_query(f'select * FROM {table_name}', engine)
            load(df, table_name)

    except Exception as e:
        print("Data extract error: " + str(e))
#load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{puid}:{pwd}@{pserver}:5432/postgres')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False, chunksize=100000)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))