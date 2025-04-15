import sqlite3

import requests
from bs4 import BeautifulSoup
import pandas as pd

headers = {
    'User-Agent': 'Mozilla/5.0 (platform; rv:gecko-version) Gecko/gecko-trail Firefox/firefox-version'
}
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

con = sqlite3.connect('C:\\Users\\rshon\\Desktop\\Data Science\\Stonks\\s&p500\\data\\snp_db.sqlite')
cur = con.cursor()
req = requests.get(url, headers=headers)

if req.status_code == 200:
    data = {'symbol': [], 'security': [], 'sector': [], 'sub_industry': [], 'hq_location': [], 'date_added': [], 'CIK': [], 'founded': []}

    soup = BeautifulSoup(req.content, 'html.parser')

    table = soup.select('#constituents')[0]
    rows = table.find_all('tr')[1:]

    for row in rows:
        tds = row.find_all('td')
        data['symbol'].append(tds[0].text.strip('\n'))
        data['security'].append(tds[1].text.strip('\n'))
        data['sector'].append(tds[2].text.strip('\n'))
        data['sub_industry'].append(tds[3].text.strip('\n'))
        data['hq_location'].append(tds[4].text.strip('\n'))
        data['date_added'].append(tds[5].text.strip('\n'))
        data['CIK'].append(tds[6].text.strip('\n'))
        data['founded'].append(tds[7].text.strip('\n'))

    df = pd.DataFrame(data).drop_duplicates(subset=['CIK'], keep='first')
    df.to_sql('snp', con, if_exists='replace', index=False)
    con.close()
else:
    print('request failed')