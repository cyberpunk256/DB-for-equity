#!/usr/bin/env python
# coding: utf-8

# In[4]:


from __future__ import print_function
import psycopg2
import os


# In[ ]:


def check_db_info(v_path):
    file = os.getcwd()
    v = open(file + v_path,"r")
    detail = v.readlines()[1:]
    detail = detail[0].split(",")
    return detail

def get_df_tickers(pat):
    with pat:
        cur = pat.cursor()
        cur.execute("SELECT id,ticker FROM symbol")
        data = cur.fetchall()
        return [d[0],d[1] for d in data]
    
def choose_dates(symbol_id,symbol,pat):
    cur = pat.cursor()
    cur.execute()
    data = cur.fetchall()
    first_date = date[0][0].strftime("%m/%d/%Y")
    last_date = date[0][1].strftime("%m/%d/%Y")
    return str.join(",",(symbol,first_date,last_date))

def main():
    db_info = "database_info.txt"
    db_info_file = "//" + db_info
    db_host, db_user, db_password, db_name = check_db_info(db_info_file_p)
    
    pat = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
    
    stock_data = get_df_tickers(pat)
    
    failed_symbols_file = '\\failed_symbols.txt'
    cur_path = os.getcwd()
    failed_symbols_file_path = cur_path + failed_symbols_file
    
    with open(failed_symbols_file_path) as f:
        failed_symbols = f.readlines()
        
    failed_symbols = [x.strip('\n') for x in failed_symbols]
    
    date_array = []
    
    for stock in stock_data:
        
    symbol_id = stock[0]
        symbol = stock[1]
        if symbol in failed_symbols:
            continue
        else:
            print('Fetching first date and last date for {}'.format(symbol))
            dates_data_string = select_first_last_dates(symbol_id, symbol, conn)
            date_array.append(dates_data_string)

            
    file_to_write = open('stock_dates.txt', 'w')

    for date_data in collect_date_array:
        file_to_write.write("%s\n" % date_data)
    
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

def create_db(db_credential_info):
   
    db_host, db_user, db_password, db_name = db_credential_info
    
    if check_db_exists(db_credential_info):
        pass
    else:
        print('Creating new database.')
        conn = psycopg2.connect(host=db_host, database='postgres', user=db_user, password=db_password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("CREATE DATABASE %s  ;" % db_name)
        cur.close()

        
def check_db_exists(db_credential_info):
    
    db_host, db_user, db_password, db_name = db_credential_info
    try:
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.close()
        print('Database exists.')
        return True
    except:
        print("Database does not exist.")
        return False

       
def create_mkt_tables(db_credential_info):
    
    db_host, db_user, db_password, db_name = db_credential_info
    conn = None
    
    if check_db_exists(db_credential_info):
        commands = ()
            try:
            for command in commands:
                print('Building tables.')
                conn = psycopg2.connect(host=db_host,database=db_name, user=db_user, password=db_password)
                cur = conn.cursor()
                cur.execute(command)
                conn.commit()
                cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.close()
        finally:
            if conn:
                conn.close()
    else:
        pass

    
def load_db_credential_info(f_name_path):
    
    cur_path = os.getcwd()
    f = open(cur_path + f_name_path, 'r')
    lines = f.readlines()[1:]
    lines = lines[0].split(',')
    return lines


def main():
    db_credential_info = "database_info.txt"
    db_credential_info_p = "\\" + db_credential_info
    
    db_host, db_user, db_password, db_name = load_db_credential_info(db_credential_info_p)
    
    create_db([db_host, db_user, db_password, db_name])
    
    create_mkt_tables([db_host, db_user, db_password, db_name])

    
from __future__ import print_function

import datetime
import psycopg2
import fix_yahoo_finance as yf
import pandas as pd
import os

MASTER_LIST_FAILED_SYMBOLS = []

def load_db_credential_info(f_name_path):
    cur_path = os.getcwd()
    f = open(cur_path + f_name_path, 'r')
    lines = f.readlines()[1:]
    lines = lines[0].split(',')
    return lines

    
def obtain_list_db_tickers(conn):
    
    with conn:
        cur = conn.cursor()
        cur.execute("SELECT id, ticker FROM symbol")
        data = cur.fetchall()
        return [(d[0], d[1]) for d in data]


def insert_new_vendor(vendor, conn):
    
    todays_date = datetime.datetime.utcnow()
    cur = conn.cursor()
    cur.execute(
                "INSERT INTO data_vendor(name, created_date, last_updated_date) VALUES (%s, %s, %s)",
                (vendor, todays_date, todays_date)
                )
    conn.commit()
    
    
def fetch_vendor_id(vendor_name, conn):
    
    cur = conn.cursor()
    cur.execute("SELECT id FROM data_vendor WHERE name = %s", (vendor_name,))
    vendor_id = cur.fetchall()
    vendor_id = vendor_id[0][0]
    return vendor_id


def load_yhoo_data(symbol, symbol_id, vendor_id, conn):
    
    cur = conn.cursor()
    start_dt = datetime.datetime(2004,12,30)
    end_dt = datetime.datetime(2017,12,1)
    
    yf.pdr_override()
    
    try:
        data = yf.download(symbol, start=start_dt, end=end_dt)
    except:
        MASTER_LIST_FAILED_SYMBOLS.append(symbol)
        raise Exception('Failed to load {}'.format(symbol))
        
    data['Date'] = data.index
    
    columns_table_order = ['data_vendor_id', 'stock_id', 'created_date', 
                           'last_updated_date', 'date_price', 'open_price',
                           'high_price', 'low_price', 'close_price',
                           'adj_close_price', 'volume']
    
    newDF = pd.DataFrame()
    newDF['date_price'] = data['Date']
    newDF['open_price'] = data['Open']
    newDF['high_price'] = data['High']
    newDF['low_price'] = data['Low']
    newDF['close_price'] = data['Close']
    newDF['adj_close_price'] = data['Adj Close']
    newDF['volume'] = data['Volume']
    newDF['stock_id'] = symbol_id
    newDF['data_vendor_id'] = vendor_id
    newDF['created_date'] = datetime.datetime.utcnow()
    newDF['last_updated_date'] = datetime.datetime.utcnow()
    newDF = newDF[columns_table_order]
    
    newDF = newDF.sort_values(by=['date_price'], ascending = True)
    
    list_of_lists = newDF.values.tolist()
    tuples_mkt_data = [tuple(x) for x in list_of_lists]
    
    insert_query =  []
    cur.executemany(insert_query, tuples_mkt_data)
    conn.commit()    
    print('{} complete!'.format(symbol))
   

def main():
    
    db_info_file = "database_info.txt"
    db_info_file_p = "\\" + db_info_file
    db_host, db_user, db_password, db_name = load_db_credential_info(db_info_file_p)
    
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
    
    stock_data = obtain_list_db_tickers(conn)
    
    vendor = 'Yahoo Finance'
    
    insert_new_vendor(vendor, conn)
    
    vendor_id = fetch_vendor_id(vendor, conn)
    
    for stock in stock_data:
        symbol_id = stock[0]
        symbol = stock[1]
        print('Currently loading {}'.format(symbol))
        try:
            load_yhoo_data(symbol, symbol_id, vendor_id, conn)
        except:
            continue
        
    file_to_write = open('failed_symbols.txt', 'w')

    for symbol in MASTER_LIST_FAILED_SYMBOLS:
        file_to_write.write("%s\n" % symbol)
     
def parse_wiki_snp500():
    
    now = datetime.datetime.utcnow()
    
    response = requests.get("http://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = bs4.BeautifulSoup(response.text)
    
    symbols_list = soup.select('table')[0].select('tr')[1:]
    
    symbols = []
    for i, symbol in enumerate(symbols_list):
        tds = symbol.select('td')
        symbols.append(
                        (tds[0].select('a')[0].text,'equity',
                         tds[1].select('a')[0].text,
                         tds[3].text, 'USD', now, now)
                      )
    return symbols


def insert_snp500_symbols_postgres(symbols, db_host, db_user, db_password, db_name):
    
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
    
    column_str = []
    insert_str = ("%s, " * 7)[:-2]
    final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
    with conn:
        cur = conn.cursor()
        cur.executemany(final_str, symbols)

        
def load_db_info(f_name_path):
    
    cur_path = os.getcwd()
    f = open(cur_path + f_name_path, 'r')
    lines = f.readlines()[1:]
    lines = lines[0].split(',')
    return lines


def main():
    db_info_file = "database_info.txt"
    db_info_file_p = "\\" + db_info_file
    db_host, db_user, db_password, db_name = load_db_info(db_info_file_p)
    
    symbols = parse_wiki_snp500()
    insert_snp500_symbols_postgres(symbols, db_host, db_user, db_password, db_name)
    print("%s symbols were successfully added." % len(symbols))  

    
if __name__ == "__main__":
    main()


