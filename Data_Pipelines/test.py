import pyodbc

# List available ODBC drivers
for driver in pyodbc.drivers():
    print(driver)