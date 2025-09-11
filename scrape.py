import urls

import time
import math

import requests
import pandas as pd

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:137.0) Gecko/20100101 Firefox/137.0'
}

def get_list():
    data = {'exchange_url': [], 'symbol': [], 'security': [], 'sector': [], 'sub_industry': [], 'hq_location': [], 'date_added': [], 'CIK': [], 'founded': []}
    
    req = requests.get(urls.snp_list_url, headers=headers)

    if req.status_code == 200:
        print('[scrape.get_list()] Request success')

        soup = BeautifulSoup(req.content, 'html.parser')
        snp = soup.select('#constituents')[0].find_all('tr')[1:]

        for stock in snp:
            tds = stock.find_all('td')
            
            data['exchange_url'].append(tds[0].find('a')['href'].strip())
            data['symbol'].append(tds[0].text.strip())
            data['security'].append(tds[1].text.strip())
            data['sector'].append(tds[2].text.strip())
            data['sub_industry'].append(tds[3].text.strip())
            data['hq_location'].append(tds[4].text.strip())
            data['date_added'].append(tds[5].text.strip())
            data['CIK'].append(tds[6].text.strip())
            data['founded'].append(tds[7].text.strip())

        df = pd.DataFrame(data).drop_duplicates(subset=['CIK'], keep='first')
        df.to_csv('data/csv/snp_list.csv')
        print('[scrape.get_list()] S&P500 list saved to /data/csv/snp_list.csv')
        
        return 0
    else:
        print(f'[scrape.get_list()] Error: code {req.status_code}')
        return -1
    

def get_nyse_month(driver, url, symbol):
    driver.get(url)

    wait = WebDriverWait(driver, timeout=5)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'flex_tr')))
    rows = driver.find_elements(By.CLASS_NAME, 'flex_tr')[:30]

    temp = []
    data = {'symbol': [], 'date': [], 'open': [], 'high': [], 'low': [], 'close':[], 'volume': []}

    for row in rows:
        temp.append(row.text.replace(',', '').split('\n'))

    for row in temp:
        data['symbol'].append(symbol)
        data['date'].append(row[0].replace('/', '-'))
        data['open'].append(float(row[1]))
        data['high'].append(float(row[2]))
        data['low'].append(float(row[3]))
        data['close'].append(float(row[4]))
        data['volume'].append(int(row[5].replace(',', '')))

    return data


def get_nasdaq_month(driver, url,  symbol):
    url = f'{url}/historical?page=1&rows_per_page=50&timeline=m6'
    driver.get(url)

    wait = WebDriverWait(driver, timeout=5)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.historical-table-container > nsdq-table:nth-child(1)')))

    table = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[2]/article/div/div[2]/div/div[2]/div[1]/div[1]/div[3]/div/div[1]/div[2]/nsdq-table')
    rows = table.text.replace(',', '').split('\n')
    rows = rows[1:31]

    temp = []
    data = {'symbol': [], 'date': [], 'open': [], 'high': [], 'low': [], 'close':[], 'volume': []}

    for row in rows:
        temp.append(row.split(' '))

    for row in temp:
        data['symbol'].append(symbol)
        data['date'].append(row[0].replace('/', '-'))
        data['open'].append(float(row[3].replace('$', '')))
        data['high'].append(float(row[4].replace('$', '')))
        data['low'].append(float(row[5].replace('$', '')))
        data['close'].append(float(row[1].replace('$', '')))
        data['volume'].append(int(row[2].replace(',', '')))
    
    return data


# base url from urls.py (urls.stock_base_url)
def get_yahoo_month(driver, symbol):
    if '.' in symbol:
        symbol = symbol.replace('.', '-')

    url = f'{urls.stock_base_url}{symbol}/history/'
    driver.get(url)

    wait = WebDriverWait(driver, timeout=4)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.table > tbody:nth-child(2)')))

    tbody = driver.find_element(By.CSS_SELECTOR, '.table > tbody:nth-child(2)')
    rows = tbody.text.replace(',', '').split('\n')

    temp = []
    data = {'symbol': [], 'date': [], 'open': [], 'high': [], 'low': [], 'close':[], 'volume': []}
    for row in rows:
        if 'Dividend' not in row and 'Splits' not in row:
            temp.append(row.split(' '))

    for row in temp:
        data['symbol'].append(symbol)
        data['date'].append(f'{row[0]}-{row[1].replace(',', '')}-{row[2]}')
        data['open'].append(float(row[3]))
        data['high'].append(float(row[4]))
        data['low'].append(float(row[5]))
        data['close'].append(float(row[6]))
        data['volume'].append(int(row[8].replace(',', '')))

    return data


def get_data(frame):
    driver = webdriver.Firefox()

    data = []
    for i, row in frame.iterrows():
        try:
            if 'nyse' in row['exchange_url']:
                data.append(get_nyse_month(driver, row['exchange_url'], row['symbol']))
                print(f'{i} NYSE: {row['symbol']}')
                time.sleep(.5)
            elif 'nasdaq' in row['exchange_url']:
                data.append(get_nasdaq_month(driver, row['exchange_url'], row['symbol']))
                print(f'{i} NASDAQ: {row['symbol']}')
                time.sleep(.5)
            elif 'yahoo' in row['exchange_url']:
                data.append(get_yahoo_month(driver, row['symbol']))
                print(f'{i} YAHOO: {row['symbol']}')
                time.sleep(.5)
        except TimeoutException:
            try:
                time.sleep(5)
                data.append(get_yahoo_month(driver, row['symbol'])) # what if this times out too...
                print(f'{i} YAHOO: {row['symbol']}')
            except TimeoutException:
                print(f'Error:{i} - {row['symbol']}')


    print(len(data))
    driver.quit()
    return data


def split_snp(snp):
    for i, row in snp.iterrows():
        if 'nyse' not in row['exchange_url'] and 'nasdaq' not in row['exchange_url']:
            snp.loc[i, 'exchange_url'] = 'yahoo'

    nyse = snp.query('exchange_url.str.contains("nyse") | exchange_url.str.contains("yahoo")').reset_index()
    nasdaq = snp.query('exchange_url.str.contains("nasdaq")').reset_index()

    nyse_mid = math.ceil(len(nyse) / 2)

    yahoo = nyse[nyse_mid:].reset_index()
    nyse = nyse[:nyse_mid].reset_index()

    data = {'exchange_url': [], 'symbol': []}
    count = 0
    nyse_i = 0
    nasdaq_i = 0
    yahoo_i = 0

    while (count < (len(nasdaq)+len(nyse)+len(yahoo))+1):
        if nasdaq_i < len(nasdaq):
            if count % 3 == 0:
                data['exchange_url'].append(nyse.loc[nyse_i, 'exchange_url'])
                data['symbol'].append(nyse.loc[nyse_i, 'symbol'])
                nyse_i += 1

            if count % 3 == 1:
                data['exchange_url'].append(nasdaq.loc[nasdaq_i, 'exchange_url'])
                data['symbol'].append(nasdaq.loc[nasdaq_i, 'symbol'])
                nasdaq_i += 1

            if count % 3 == 2:
                data['exchange_url'].append('yahoo')
                data['symbol'].append(yahoo.loc[yahoo_i, 'symbol'])
                yahoo_i += 1
        else:
            if count % 2 == 0 and nyse_i < len(nyse):
                data['exchange_url'].append(nyse.loc[nyse_i, 'exchange_url'])
                data['symbol'].append(nyse.loc[nyse_i, 'symbol'])
                nyse_i += 1

            if count % 2 == 1 and yahoo_i < len(yahoo):
                data['exchange_url'].append('yahoo')
                data['symbol'].append(yahoo.loc[yahoo_i, 'symbol'])
                yahoo_i += 1
        count += 1

    return data