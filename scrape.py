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
months = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}

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
    

def get_nyse_ytd(driver, url, symbol, year_start='01-02-2025'): # not correct date format for nyse
    driver.get(url)

    wait = WebDriverWait(driver, timeout=5)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'flex_tr')))
    rows = driver.find_elements(By.CLASS_NAME, 'flex_tr')

    temp = []
    data = {'symbol': [], 'date': [], 'open': [], 'high': [], 'low': [], 'close':[], 'volume': []}

    wait = WebDriverWait(driver, timeout=6.5)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.d-dquote-datablock:nth-child(9) > span:nth-child(2)')))

    today_open = float(driver.find_element(By.CSS_SELECTOR, 'div.d-dquote-datablock:nth-child(9) > span:nth-child(2)').text.replace(',', ''))
    today_high = float(driver.find_element(By.CSS_SELECTOR, 'div.d-dquote-datablock:nth-child(10) > span:nth-child(2)').text.replace(',', ''))
    today_low = float(driver.find_element(By.CSS_SELECTOR, 'div.d-dquote-datablock:nth-child(11) > span:nth-child(2)').text.replace(',', ''))
    today_volume = int(driver.find_element(By.CSS_SELECTOR, 'div.d-dquote-datablock:nth-child(4) > span:nth-child(2)').text.replace(',', ''))
    today_close = float(driver.find_element(By.CLASS_NAME, 'd-dquote-x3').text.replace(',', ''))
    today_date = driver.find_element(By.CSS_SELECTOR, '.d-dquote-time > span:nth-child(2)').text.split(' ')[0].replace('/', '-')

    for row in rows:
        temp.append(row.text.replace(',', '').split('\n'))

    # check if we need to get today's info separately.
    if today_date == temp[0][0].replace('/', '-'):
        for row in temp:
            data['symbol'].append(symbol)
            data['date'].append(row[0].replace('/', '-'))
            data['open'].append(float(row[1]))
            data['high'].append(float(row[2]))
            data['low'].append(float(row[3]))
            data['close'].append(float(row[4]))
            data['volume'].append(int(row[5].replace(',', '')))

            if row[0].replace('/', '-') == year_start:
                break
    else:
        data['symbol'].append(symbol)
        data['date'].append(today_date)
        data['open'].append(today_open)
        data['high'].append(today_high)
        data['low'].append(today_low)
        data['close'].append(today_close)
        data['volume'].append(today_volume)

        for row in temp[1:]:
            data['symbol'].append(symbol)
            data['date'].append(row[0].replace('/', '-'))
            data['open'].append(float(row[1]))
            data['high'].append(float(row[2]))
            data['low'].append(float(row[3]))
            data['close'].append(float(row[4]))
            data['volume'].append(int(row[5].replace(',', '')))

            if row[0].replace('/', '-') == year_start:
                break

    return data


# base url from urls.py (urls.stock_base_url)
def get_yahoo_ytd(driver, symbol, year_start='Jan-2-2025'):
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

        if f'{row[0]}-{row[1].replace(',', '')}-{row[2]}' == year_start:
            break

    return data


# this gets all necessary data of all stocks on S&P500
# saves 'raw' data as csv and then returns it.
def get_ytd(frame):
    options = webdriver.FirefoxOptions()
    options.page_load_strategy = 'eager'
    options.add_argument('--headless')
    driver = webdriver.Firefox(options=options)

    data = []
    for i, row in frame.iterrows():
        try:
            if 'nyse' in row['exchange_url']:
                data.append(get_nyse_ytd(driver, row['exchange_url'], row['symbol']))
                print(f'{i} NYSE: {row['symbol']}')
                #time.sleep(.5)
            elif 'yahoo' in row['exchange_url']:
                data.append(get_yahoo_ytd(driver, row['symbol']))
                print(f'{i} YAHOO: {row['symbol']}')
                time.sleep(.5)
        except TimeoutException:
            try:
                time.sleep(2)
                data.append(get_yahoo_ytd(driver, row['symbol'])) # what if this times out too...
                print(f'{i} YAHOO: {row['symbol']}')
            except TimeoutException:
                print(f'Error:{i} - {row['symbol']}')

    print(len(data))
    driver.quit()

    df_list = [pd.DataFrame(x) for x in data]
    theta = pd.concat(df_list)
    theta.to_csv('data/csv/snp_YTD.csv')

    return theta


def split_snp(snp):
    for i, row in snp.iterrows():
        if 'nyse' not in row['exchange_url'] and 'nasdaq' not in row['exchange_url']:
            snp.loc[i, 'exchange_url'] = 'yahoo'

    nyse = snp.query('exchange_url.str.contains("nyse") | exchange_url.str.contains("yahoo")').reset_index()
    nasdaq = snp.query('exchange_url.str.contains("nasdaq")').reset_index()

    for i, row in nasdaq.iterrows():
        nasdaq.loc[i, 'exchange_url'] = 'yahoo'
    x = 250-len(nasdaq)

    yahoo = nyse[:x]
    nyse = nyse[x:].reset_index()

    yahoo = pd.concat([yahoo, nasdaq]).reset_index()

    data = {'exchange_url': [], 'symbol': []}
    count = 0
    nyse_i = 0
    yahoo_i = 0

    while count < len(snp):
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