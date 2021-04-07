import sys
import pyodbc
import urllib
import textwrap
import pandas as pd 
from sqlalchemy import event
from sqlalchemy import create_engine
from configparser import ConfigParser


# Read config file
config = ConfigParser()
config.read('config/config.ini')

# Define User Info
SERVER_NAME = config.get('main', 'SERVER_NAME')
DATABASE_NAME = config.get('main', 'DATABASE_NAME')
USERNAME = config.get('main', 'USERNAME')
PASSWORD = config.get('main', 'PASSWORD')

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
    Connection Timeout=3;
'''.format(
    driver=driver,
    server=server,
    database=DATABASE_NAME,
    username=USERNAME,
    password=PASSWORD
))

def connect(connection_string: str, retries: int) -> None:
    ''' Connect to the Azure SQL database server '''
    for _ in range(retries):
        print('\nConnecting to Database...')
        try:
            cnxn: pyodbc.Connection = pyodbc.connect(connection_string)    
            print(f'Database Connection Successful!!!')
            return cnxn
        except(pyodbc.Error):
            print('\nUnable to Establish Database Connection Retrying...')
            pass
    else:
        print('\nUnable to Establish Database Connection Exiting...')
        sys.exit(1)

def get_col_dtypes(dataTypes=None):
    ''' Get data type for each column in Pandas DataFrame'''
    dataList = []
    for x in dataTypes:
        if(x == 'int64'):
            dataList.append('INT')
        elif (x == 'float64'):
            dataList.append('FLOAT')
        elif (x == 'bool'):
            dataList.append('BOOLEAN')
        else:
            dataList.append('NVARCHAR(50)')
    return dataList

def df_to_sql_table(df=None, table_name=None):
    ''' Create a table statement from multi-index Pandas DataFrame '''
    print('Creating {table_name} Table Statement...')
    col_names = list(df.reset_index().columns.values)
    col_dtypes = get_col_dtypes(df.reset_index().dtypes)
    create_table = f'CREATE TABLE [{DATABASE_NAME}].[dbo].[{table_name}] ('
    for i in range(len(col_dtypes)):
        create_table = create_table + '\n' + f'[{col_names[i]}] '+ ' ' + col_dtypes[i] + ','
    create_table = create_table[:-1] + ' );'
    print(f'\nTable Statement: \n{create_table}')
    return create_table

def insert_table(df=None, table_name=None):
    ''' Add new table to SQL database from Pandas DataFrame '''
    # Create New PYODBC Connection Object
    cnxn = connect(
        connection_string=connection_string, 
        retries=3
        )
    # Create Cursor Object From Connection
    crsr: pyodbc.Cursor = cnxn.cursor()
    # Create table statement
    table_statement = df_to_sql_table(
        df=df, 
        table_name=table_name
        )
    print(f'\nAdding {table_name} Table to Database...')
    # Execute table statement
    crsr.execute(table_statement)
    # Commmit Transaction
    crsr.commit()
    # Close Connection
    cnxn.close()
    return print('\nDone...')

def df_bulk_insert(df=None, table_name=None):
    ''' Bulk DataFrame insert into SQL Database '''
    # Insert new table
    insert_table(
        df=df, 
        table_name=table_name
        )
    # Database connection parameters
    params = 'DRIVER=' + driver + ';SERVER=' + server +\
             ';PORT=1433;DATABASE=' + DATABASE_NAME +\
             ';UID=' + USERNAME + ';PWD=' + PASSWORD
    db_params = urllib.parse.quote_plus(params)
    # Create connetion engine 
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(db_params))
    # https://medium.com/analytics-vidhya/speed-up-bulk-inserts-to-sql-db-using-pandas-and-python-61707ae41990
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
            ):
                if executemany:
                    cursor.fast_executemany = True
    print('\nUploading data...')
    # Upload DataFrame to database
    df.round(5).to_sql(
        name=table_name, 
        con=engine, 
        index=True, 
        if_exists="append", 
        schema="dbo"
        )
    return print('\nDone!!!')

def fetch_df(table_name: str) -> None:
    ''' Load multi-index DataFrame from SQL table '''
    # Create New PYODBC Connection Object
    cnxn = connect(
        connection_string=connection_string, 
        retries=3
        )
    # Last date query
    query = f'SELECT * FROM {table_name}'
    # Read data from database to pandas
    data = pd.read_sql(query, cnxn).set_index(['date','symbol'])
    return data

def table_last_date(table_name: str) -> None:
    ''' Get the last date of a multi-index database table '''
    # Create New PYODBC Connection Object
    cnxn = connect(
        connection_string=connection_string, 
        retries=3
        )
    # Last date query
    query = f'SELECT * FROM {table_name} WHERE date = (SELECT max(date) FROM {table_name})'
    # Read data from database to pandas
    date = pd.read_sql(query, cnxn).set_index(['date','symbol']).index.levels[0].values[0]
    return date