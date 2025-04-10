# Code for ETL operations on Country-GDP data

from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3


def log_progress(message):
    """Logs progress messages with timestamps to code_log.txt"""
    time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open("code_log.txt", "a") as log_file:
        log_file.write(f"{time_stamp} : {message}\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', class_="wikitable")
    rows = table.find_all('tr')

    data = []
    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) >= 3:
            bank_name = cols[1].text.strip()
            market_cap = cols[2].text.strip().replace('\n', '').replace(',', '')
            try:
                market_cap = float(market_cap)
                data.append([bank_name, market_cap])
            except ValueError:
                continue

    df = pd.DataFrame(data, columns=table_attribs)

    log_progress("Data extraction complete. Initiating Transformation process")
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = dict(zip(exchange_df.iloc[:, 0], exchange_df.iloc[:, 1]))

    # Add transformed columns
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    log_progress("Data transformation complete. Initiating Loading process")
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")
    sql_connection.commit()

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)

    results = cursor.fetchall()

    print(f"Query: {query_statement}")
    for row in results:
        print(row)

    log_progress("Process Complete")


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("Preliminaries complete. Initiating ETL process")

data_url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]

df_extracted = extract(data_url, table_attribs)

csv_path="./exchange_rate.csv"

df_transformed = transform(df_extracted, csv_path)

output_path = "./Largest_banks_data.csv"

load_to_csv(df_transformed, output_path)

sql_connection = sqlite3.connect("Banks.db")
log_progress("SQL Connection initiated")
table_name = "Largest_banks"
load_to_db(df_transformed, sql_connection, table_name)

query_1 = "SELECT * FROM Largest_banks"
run_query(query_1, sql_connection)

query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_2, sql_connection)

query_3 = "SELECT Name FROM Largest_banks LIMIT 5"
run_query(query_3, sql_connection)


sql_connection.close()
log_progress("Server Connection closed")
