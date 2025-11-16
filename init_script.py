# THIS SCRIPT SHOULD BE THE VERY FIRST SCRIPT TO RUN AND SHOULD ONLY RUN ONCE!
import scrape
import db
import data_util

import pandas as pd

# init db (THIS WILL DROP ANY TABLES THAT EXIST)
db.init_db()

# scrape S&P500 table from wikipedia then insert into db, also save into csv
snp_list = scrape.get_list()
snp_list.to_csv('data/csv/snp_list.csv')
db.insert_new_snp(snp_list)
symbols = snp_list['symbol']

# scrape year to date data from yahoo then put result in dataframe
ytd_data = scrape.get_data(symbols)
df = pd.DataFrame(ytd_data)

# calculate move percentages
ytd_with_moves = data_util.calc_moves(symbols, df)

# insert year to date data into db
db.insert_stocks(ytd_with_moves)

# print 'finished'
print("Scrape complete.")