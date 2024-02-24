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

# Establish Connection
print("connecting to database.......")
        # Connection string
conn_str = f'mssql+pyodbc://{uid}:{pwd}@{server}/{database}?driver={driver}'
        # Create SQLAlchemy engine
engine = create_engine(conn_str)
print("connection successful")

# Read Data 
source = pd.read_sql_query(""" SELECT top 10
CustomerKey,GeographyKey,CustomerAlternateKey,Title,FirstName,MiddleName,LastName,NameStyle,BirthDate,MaritalStatus
FROM dbo.DimCustomer; """, engine)
print(source)

# Load initial Data to Target
# Save the data to destination as the intial load. On the first run we load all data.
tbl_name = "stg_IncrementalLoadTest"
source.to_sql(tbl_name, engine, if_exists='replace', index=False)
print("Saved to staging successful")

print("Read Data into DF")
# Read Target data into a dataframe
target = pd.read_sql('Select * from "stg_IncrementalLoadTest"', engine)
print(target)
print("successful")

# Let's select two additional rows from the source. We have two new records
source = pd.read_sql_query(""" SELECT top 12
CustomerKey,GeographyKey,CustomerAlternateKey,Title,FirstName,MiddleName,LastName,NameStyle,BirthDate,MaritalStatus
FROM dbo.DimCustomer; """, engine)
print(source)

#Read Update Source Data
#Update a Source Record. Serve as a modified row
# Also update a record. I will update the middle name for customerkey: 11006
source.loc[source.MiddleName =='G', ['MiddleName']] = 'Gina'
print(source)

#Detect Changes in data by comparing source and target
target.apply(tuple,1)
source.apply(tuple,1).isin(target.apply(tuple,1))
print(source)

# detech changes. Get rows that are not present in the target.
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
print(changes)

# Get modified rows
modified = changes[changes.CustomerKey.isin(target.CustomerKey)]
print(modified)

# Get new records
inserts = changes[~changes.CustomerKey.isin(target.CustomerKey)]
print(inserts)

def update_to_sql(df, table_name, key_name):
    a = []
    table = table_name
    primary_key = key_name
    temp_table = f"{table_name}_temporary_table"
    
    for col in df.columns:
        if col == primary_key:
            continue
        a.append(f'[{col}]=s.[{col}]')  # Use square brackets for quoting

    df.to_sql(temp_table, engine, if_exists='replace', index=False)

    update_stmt_1 = f'UPDATE [public].[{table}] '  # Removed alias 'f'
    update_stmt_2 = "SET "
    update_stmt_3 = ", ".join(a)
    update_stmt_4 = f' FROM [public].[{table}] t '  # Updated to use square brackets
    update_stmt_5 = f' INNER JOIN (SELECT * FROM [public].[{temp_table}]) AS s ON s.[{primary_key}]=t.[{primary_key}]'  # Updated to use square brackets
    update_stmt_6 = f' WHERE t.[{primary_key}]=s.[{primary_key}]'  # Updated to use square brackets
    update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 + update_stmt_6 + ";"

    print(update_stmt_7)

    with engine.begin() as cnx:
        cnx.execute(update_stmt_7)

# Call update function
update_to_sql(modified, "stg_IncrementalLoadTest", "CustomerKey")

target = pd.read_sql('Select * from "stg_IncrementalLoadTest"', engine)
print(target)

# insert new rows into the destination table
inserts.to_sql(tbl_name, engine, if_exists='append', index=False)

    