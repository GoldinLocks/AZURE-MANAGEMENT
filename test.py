import textwrap
import pandas as pd
from database import azure_db_utils
from configparser import ConfigParser


# Read config file
config = ConfigParser()
config.read('config/config.ini')

# Define database Info
DATABASE_NAME = config.get('main', 'DATABASE_NAME')

# Get list of database table names
db_tables = azure_db_utils.get_table_names(db_name=DATABASE_NAME)
print(f'\n{DATABASE_NAME} Tables: \n{db_tables}')

# Read predictions csv to DataFrame
print('\nPredictions DF:') 
predictions_df = pd.read_csv('./data/predictions.csv', index_col=[0,1])
print(predictions_df.info(verbose=False))
print(predictions_df.tail(3))

# Define new table name
pred_table = 'TD_ML_Bot_Preds'

# Load DataFrame to SQL databse table 
azure_db_utils.df_bulk_insert(
    df=predictions_df, 
    table_name=pred_table,
    new_table=True
    )

# Load eng_fts database table into DataFrame 
df = azure_db_utils.fetch_df(table_name='TD_ML_Bot_Eng_Fts')
print('\nEngineered Features DF SQL Table:') 
print(df.info(verbose=False))
print(df.tail(3))

# Get last date in database table
last_date = azure_db_utils.table_last_date(table_name ='TD_ML_Bot_Eng_Fts')
print(f'\nLast Date: \n{last_date}')