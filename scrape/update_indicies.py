import secret_info

import sqlite3

import requests
from bs4 import BeautifulSoup
import pandas as pd

headers = {
    'User-Agent': 'Mozilla/5.0 (platform; rv:gecko-version) Gecko/gecko-trail Firefox/firefox-version'
}

con = sqlite3.connect('C:\\Users\\rshon\\Desktop\\Data Science\\Stonks\\s&p500\\data\\snp_db.sqlite')

req = requests.get(secret_info.snp_url, headers=headers)

data = {'index_name': [], 'current_price': [], 'previous_price': []}
# get snp current/previous prices
if req.status_code == 200:
    soup = BeautifulSoup(req.content, 'html.parser')

    data['index_name'].append('S&P500')

    current_price = soup.select('div.yf-16vvaki > div:nth-child(1) > span:nth-child(1)')[0].text.strip().replace(',', '')
    previous_price = soup.select('li.yf-1jj98ts:nth-child(1) > span:nth-child(2) > fin-streamer:nth-child(1)')[0].text.strip().replace(',', '')
    data['current_price'].append(current_price)
    data['previous_price'].append(previous_price)
else:
    print(f'error: {req.status_code}')

df = pd.DataFrame(data)
df.to_sql('index_data', con, if_exists='replace', index=False)