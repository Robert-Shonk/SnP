import sqlite3

import numpy as np


def init_db():
    con = sqlite3.connect('data/database/snp.db')
    cur = con.cursor()

    create_snp_table(cur)
    create_stock_table(cur)

    con.commit()
    con.close()
    print('Database initialized.')
 

def create_snp_table(cursor):
    with open('data/database/snp_table.sql', 'r') as f:
        script = f.read()

    cursor.executescript(script)


def create_stock_table(cursor):
    with open('data/database/stock_table.sql', 'r') as f:
        script = f.read()

    cursor.executescript(script)


def drop_table(table_names):
    con = sqlite3.connect('data/database/snp.db')
    cur = con.cursor()

    if type(table_names) == list:
        for table in table_names:
            cur.execute(f'DROP TABLE IF EXISTS {table}')
            con.commit()
    else:
        stm = f'DROP TABLE IF EXISTS {table_names}'
        cur.execute(stm)
        con.commit()

    con.close()


def insert_new_snp(new_snp):
    drop_table('snp')
    create_snp_table()
    
    con = sqlite3.connect('data/database/snp.db')
    cur = con.cursor()
    
    data = []

    for i, row in new_snp.iterrows():
        data.append(tuple(row)[1:])

    cur.executemany('INSERT INTO snp (exchange_url, symbol, security, sector, sub_industry, hq_location, date_added, CIK, founded) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', data)
    con.commit()
    print('snp table updated.')


def insert_stocks(stocks):
    con = sqlite3.connect('data/database/snp.db')
    cur = con.cursor()

    data = []

    for i, row in stocks.iterrows():
        d = []
        for c in row:
            if type(c) == np.int64:
                d.append(int(c))
            elif type(c) == np.float64:
                d.append(float(c))
            else:
                d.append(c)
                
        data.append(d)

    cur.executemany('INSERT INTO stock (symbol, date, open, high, low, close, volume, move) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', data)
    con.commit()

    con.close()
    print('YTD stock data added.')