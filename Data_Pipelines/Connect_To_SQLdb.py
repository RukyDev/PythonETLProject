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

    # Query Table
sql = "SELECT * FROM [dbo].[FactProductInventory]"
df = pd.read_sql_query(sql, conn)
print(df.head())
print("Saving to Excel......") 
# Save the DataFrame to an Excel file
df.to_excel('FactProductInventory.xlsx', index=False) 
print("Saved to Excel")
