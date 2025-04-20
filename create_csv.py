import sqlite3

import pandas as pd

def get_sectors():
    con = sqlite3.connect('data\\snp_db.sqlite')
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        """
            SELECT snp.sector, ROUND(SUM(stock.market_cap)*1000000000) AS market_cap, ROUND(((SUM(stock.current_price)/SUM(stock.previous_price))-1)*100, 2) AS move FROM stock
            JOIN snp ON stock.symbol = snp.symbol
            GROUP BY snp.sector
            ORDER BY move DESC;
        """
    ).fetchall()
    con.close()
    
    df = pd.DataFrame(rows, columns=rows[0].keys())
    df.to_csv('sectors.csv', index=False)
    print('Created "sectors.csv"')

def get_stocks():
    con = sqlite3.connect('data\\snp_db.sqlite')
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        """
            SELECT snp.security, stock.*, snp.sector, ROUND(((stock.current_price/stock.previous_price)-1)*100, 2) AS move,
            CASE
                WHEN ((stock.current_price/stock.previous_price)-1)*100 >= 0 THEN 'up'
                ELSE 'down'
            END AS category
            FROM stock
            JOIN snp ON stock.symbol = snp.symbol
            ORDER BY move DESC;
        """
    ).fetchall()
    con.close()

    df = pd.DataFrame(rows, columns=rows[0].keys())
    df.to_csv('stocks.csv', index=False)
    print('Created "stocks.csv"')

def get_snp():
    con = sqlite3.connect('data\\snp_db.sqlite')
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        """
            SELECT index_name, current_price, previous_price, ROUND(((current_price/previous_price)-1)*100, 2) AS move,
            (current_price - previous_price) AS gain
            FROM index_data;
        """
    ).fetchall()
    con.close()

    df = pd.DataFrame(rows, columns=rows[0].keys())
    df.to_csv('index_data.csv', index=False)
    print('Created "index_data.csv"')


get_sectors()
get_stocks()
get_snp()