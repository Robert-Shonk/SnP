import secret_info

import time
import random
import sqlite3

import requests
from bs4 import BeautifulSoup
import pandas as pd


headers = {
    'User-Agent': 'Mozilla/5.0 (platform; rv:gecko-version) Gecko/gecko-trail Firefox/firefox-version'
}

con = sqlite3.connect('C:\\Users\\rshon\\Desktop\\Data Science\\Stonks\\s&p500\\data\\snp_db.sqlite')
cur = con.cursor()
symbols = cur.execute('SELECT symbol FROM snp').fetchall()

data = {'symbol': [], 'market_cap': [], 'current_price': [], 'previous_price': [], 'day_low': [], 'day_high': [], 'year_week_low': [], 'year_week_high': []}

total_time = 0
count = 1
for symbol in symbols:
    start = time.time()
    req = requests.get(secret_info.url1 + symbol[0].replace('.', '-'), headers=headers)

    # need to reconsider how i want to handle persistence of data in db. do i want to add new rows each day or just update each row with new data each day.
    # let's start with replacing each day...
    if req.status_code == 200:
        soup = BeautifulSoup(req.content, 'html.parser')
        
        data['symbol'].append(symbol[0])
        data['market_cap'].append(soup.select('li.yf-1jj98ts:nth-child(9) > span:nth-child(2) > fin-streamer:nth-child(1)')[0].text.strip().replace(',', ''))
        data['current_price'].append(float(soup.select('div.yf-16vvaki > div:nth-child(1) > span:nth-child(1)')[0].text.strip().replace(',', '')))
        data['previous_price'].append(float(soup.select('li.yf-1jj98ts:nth-child(1) > span:nth-child(2) > fin-streamer:nth-child(1)')[0].text.strip().replace(',', '')))

        low_high = soup.select('li.yf-1jj98ts:nth-child(5) > span:nth-child(2) > fin-streamer:nth-child(1)')[0].text.split(' - ')
        data['day_low'].append(float(low_high[0].replace(',', '')))
        data['day_high'].append(float(low_high[1].replace(',', '')))

        year_low_high = soup.select('li.last-md:nth-child(6) > span:nth-child(2) > fin-streamer:nth-child(1)')[0].text.split(' - ')
        data['year_week_low'].append(float(year_low_high[0].replace(',', '')))
        data['year_week_high'].append(float(year_low_high[1].replace(',', '')))

        print(f'[Success] {count}: {symbol[0]}')
        count += 1
    else:
        print(f'[Error] Response code: {req.status_code}')
        print(f'Symbol: {symbol[0]}')
    
    time.sleep(random.randint(1, 4))
    end = time.time()
    total_time += (end-start)
    print(f'Time taken: {end - start}')

print(f'Scrape complete. Total time: {total_time/60} minutes.')

df = pd.DataFrame(data)

market_cap_billions = []
for row in range(len(df)):
    mc = df.iloc[row]['market_cap']

    if 'B' in mc:
        mc_float = float(mc.strip('B'))

    elif 'T' in mc:
        mc_float = float(mc.strip('T'))*1000

    market_cap_billions.append(mc_float)

df['market_cap'] = market_cap_billions

if len(df) == 500:
    df.to_sql('stock', con, if_exists='replace', index=False)
    print('Saved to database.')
else:
    print('Failed to load to database. Missing data.')
    print('Saving results to csv...')
    df.to_csv('failed_scrape.csv', index=False)
con.close()