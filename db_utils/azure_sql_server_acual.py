import textwrap
import pyodbc

# Specify Driver
driver = '{ODBC Driver 17 for SQL Server}'

# Specify Server & Database Name
server_name = 'ml-data-server'
database_name = 'trading-sql-db'

# Create Server URL
server = '{server_name}.database.windows.net,1433'.format(server_name=server_name)

# Define Username & Password
username = 'srose'
password = '1v0717U7'

# Full Connection String
connection_string = textwrap.dedent('''
    Driver={driver};
    Server={server};
    Database={database};
    Uid={username};
    Pwd={password};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;
'''.format(
    driver=driver,
    server=server,
    database=database_name,
    username=username,
    password=password
))

# Create New PYODBC Connection Object
cnxn: pyodbc.Connection = pyodbc.connect(connection_string)

# Create Cursor Object Frrom Cnnection
crsr: pyodbc.Cursor = cnxn.cursor()

# Select Query
select_sql = 'SELECT * FROM [news_articles_cnbc]'

# Execute Select Query
crsr.execute(select_sql)

# Insert Query
insert_sql = 'INSERT INTO [news_articles_cnbc] (news_id, news_source, guid) VALUES (?, ?, ?)'

# Define Recordset
records = [
    ('ABC123', 'cnbc', '400'),
    ('ABC369', 'yahoo', '250')
]

# Define Data Types of Input Values
crsr.setinputsizes(
    [
        (pyodbc.SQL_VARCHAR, 50, 0),
        (pyodbc.SQL_VARCHAR, 50, 0),
        (pyodbc.SQL_VARCHAR, 50, 0)
    ]
)

# Execute Insert Statement
crsr.executemany(insert_sql, records)

# Commmit Transaction
crsr.commit()

# Close Connection
cnxn.close()