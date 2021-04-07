import textwrap
import pandas as pd
from database import azure_db_utils
from configparser import ConfigParser


# Read config file
config = ConfigParser()
config.read('config/config.ini')

# Define database Info
DATABASE_NAME = config.get('main', 'DATABASE_NAME')

# Read base_fts csv to DataFrame
print('\nBase Features DF:') 
base_fts_df = pd.read_csv('./data/base_features.csv', index_col=[0,1])
print(base_fts_df.info(verbose=False))
print(base_fts_df.tail(3))

# Load DataFrame to SQL databse table 
azure_db_utils.df_bulk_insert(
    df=base_fts_df, 
    table_name='TD_ML_Bot_base_Fts'
    )

# Load eng_fts database table to DataFrame 
df = azure_db_utils.fetch_df(table_name='TD_ML_Bot_Eng_Fts')
print('\nEngineered Features DF SQL Table:') 
print(df.info(verbose=False))
print(df.tail(3))

# Get last date in database table
last_date = azure_db_utils.table_last_date(table_name ='TD_ML_Bot_Eng_Fts')
print(f'\nLast Date: \n{last_date}')

db_tables = azure_db_utils.get_table_names(db_name=DATABASE_NAME)
print(f'{DATABASE_NAME} Tables: \n{db_tables}')