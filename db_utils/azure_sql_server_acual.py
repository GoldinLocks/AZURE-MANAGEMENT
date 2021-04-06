import textwrap
import pyodbc
import pandas as pd 
from configparser import ConfigParser

config = ConfigParser()
config.read('config/config.ini')

# Define User Info
SERVER_NAME = config.get('main', 'SERVER_NAME')
DATABASE_NAME = config.get('main', 'DATABASE_NAME')
USERNAME = config.get('main', 'USERNAME')
PASSWORD = config.get('main', 'PASSWORD')

conf_df = pd.DataFrame.from_dict({'SERVER_NAME': SERVER_NAME,
                                  'REDIRECT_URI': DATABASE_NAME,
                                  'ACCOUNT_NUMBER': USERNAME,
                                  'CREDENTIALS_PATH': PASSWORD}, 
                                  orient='index', columns=['USER_INFO:'])

print()
print(conf_df)
print()

# Specify Driver
driver = '{ODBC Driver 17 for SQL Server}'

# Create Server URL
server = '{server_name}.database.windows.net,1433'.format(server_name=SERVER_NAME)

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
    database=DATABASE_NAME,
    username=USERNAME,
    password=PASSWORD
))

def connect(connection_string: str) -> None:
    ''' Connect to the Azure SQL database server '''
    cnxn = None
    while True:
        print('\nConnecting to Database...')
        print(f'\nDatabase Connection: \n{connection_string}')
        cnxn: pyodbc.Connection = pyodbc.connect(connection_string)
        if cnxn == None:
            pass
        else:
            print(f'\nDatabase Connection Object: {str(cnxn)}')
            return cnxn

# Create New PYODBC Connection Object
cnxn = connect(connection_string=connection_string)

# Create Cursor Object From Connection
crsr: pyodbc.Cursor = cnxn.cursor()

# Select Query
select_sql = 'SELECT * FROM [news_articles_cnbc]'

# Execute Select Query
crsr.execute(select_sql)

# Insert Query
insert_sql = 'INSERT INTO [news_articles_cnbc] (news_id, news_source, guid) VALUES (?, ?, ?)'

# Define Records
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