import pandas as pd
from sqlalchemy import create_engine

# MSSQL Server connection parameters for local server
uid = 'ruky'
pwd = 'Emmanuel@68'
server = 'RUKYDEV\RUKYDEV'
database = 'AdventureWorksDW2019'
driver = 'ODBC Driver 17 for SQL Server'

# Connection string
conn_str = f'mssql+pyodbc://{uid}:{pwd}@{server}/{database}?driver={driver}'

# Create SQLAlchemy engine
engine = create_engine(conn_str)

# Load data from Excel to a Pandas DataFrame
excel_file_path = r'C:\Users\HP\Documents\Data_Engineering\PythonETLProject\FactProductInventory.xlsx'
df = pd.read_excel(excel_file_path)
print(df.head())

# Delete existing records in the MSSQL Server table
print('Deleting Existing Records.....')
with engine.connect() as connection:
    connection.execute('DELETE FROM dbo.ProductInventory')

# Load data into MSSQL Server table
print('Loading New Records......')
df.to_sql('ProductInventory', con=engine, schema='dbo', index=False, if_exists='append')
print('Records Loaded Successfully.')
