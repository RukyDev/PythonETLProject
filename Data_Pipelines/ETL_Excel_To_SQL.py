import pyodbc
import pandas as pd
import os

uid = 'ruky'
pwd = 'Emmanuel@68'
server = 'RUKYDEV'
database = 'AdventureWorksDW2019'
driver = 'ODBC Driver 17 for SQL Server'


# Connection
conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '\RUKYDEV' + 
                      ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd)
print('Connected to Database')

# Load data from Excel to a Pandas DataFrame
excel_file_path = r'C:\Users\HP\Documents\Data_Engineering\PythonETLProject\Data_Pipelines\FactProductInventory.xlsx'
df = pd.read_excel(excel_file_path)
print(df.head())

 # Delete existing records in the MSSQL Server table
print('Deleting Existing Records.....')
cursor = conn.cursor()
cursor.execute('DELETE FROM dbo.ProductInventory')
conn.commit()
print('Record Deleted.')

# Load data into MSSQL Server table
print('Loading New Recored......')
df.to_sql('dbo.ProductInventory', conn, schema='dbo', index=False, if_exists='append')
print('Record Loaded Successfully.')