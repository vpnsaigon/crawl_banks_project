# Code for ETL operations on Country-GDP data
# wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 


# Task 1: Logging function
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file_name,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')  


# Task 2: Extraction of data
def extract(url):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame()
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[1].find('a') is not None:
                data_dict = {"Name": col[1].text[:-1],
                             "MC_USD_Billion": float(col[2].text[:-1])}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df if not df.empty else None, df1], ignore_index=True)
    return df


# Task 3: Transformation of data
def transform(df, exchange_rate_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    df_exchange_rate = pd.read_csv(exchange_rate_path)
    df_exchange_rate = df_exchange_rate.set_index('Currency').to_dict()['Rate']
    
    USD_list = df["MC_USD_Billion"].tolist()
    GBP_list = [np.round(x*df_exchange_rate["GBP"],2) for x in USD_list]
    EUR_list = [np.round(x*df_exchange_rate["EUR"],2) for x in USD_list]
    INR_list = [np.round(x*df_exchange_rate["INR"],2) for x in USD_list]
    
    df['MC_GBP_Billion'] = GBP_list
    df['MC_EUR_Billion'] = EUR_list
    df['MC_INR_Billion'] = INR_list

    return df


# Task 4: Loading to CSV
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    
    df.to_csv(output_path)


# Task 5: Loading to Database
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


# Task 6: Function to Run queries on Database
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    print('')


# Task 7: Verify log entries
# rm code_log.txt

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
exchange_rate_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'
log_file_name = './code_log.txt'


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, exchange_rate_path)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Executing queries')

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')

query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')

sql_connection.close()
log_progress('Server Connection closed.')

