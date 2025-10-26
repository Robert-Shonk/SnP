import urls

import time

import requests
import pandas as pd

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException


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
    

def scrape_row(row, symbol):
    data = {}
    raw_data = row.text
    if 'Dividend' not in raw_data and 'Splits' not in raw_data:
        spl = raw_data.split(' ')
        data['symbol'] = symbol.replace('-', '.')

        # format date to YYYY-MM-DD for sqlite
        year = spl[2]
        month = months[spl[0]]
        day = spl[1].replace(',', '')
        if len(day) < 2:
            day = f'0{day}'

        data['date'] = '-'.join([year, month, day])

        data['open'] = float(spl[3].replace(',', ''))
        data['high'] = float(spl[4].replace(',', ''))
        data['low'] = float(spl[5].replace(',', ''))
        data['close'] = float(spl[6].replace(',', ''))
        data['volume'] = int(spl[8].replace(',', ''))
        
    return data


def get_rows(driver, sym):
    url = f'https://finance.yahoo.com/quote/{sym}/history'
    driver.get(url)

    try:
        table = driver.find_element(By.CSS_SELECTOR, '.table')
        rows = table.find_elements(By.TAG_NAME, 'tr')[1:]
    except NoSuchElementException:
        try:
            rows = driver.find_elements(By.TAG_NAME, 'tr')[1:]
        except NoSuchElementException:
            print(f'[NoSuchElementException] Symbol: {sym}. Could not find elements with data.')

    return rows


# returns list of dictionaries
def get_ytd(symbols, year_start='2025-01-02'):
    options = webdriver.ChromeOptions()
    options.page_load_strategy = 'eager'
    options.add_argument('--headless=new')
    driver = webdriver.Chrome(options=options)

    d = []
    count = 1
    for sym in symbols:
        sym = sym.replace('.', '-')
        t0 = time.time()

        rows = get_rows(driver, sym)

        if len(rows) > 0:
            for row in rows:
                parsed = scrape_row(row, sym)
                
                if len(parsed) > 0:
                    d.append(parsed)

                    if parsed['date'] == year_start:
                        break
        else:
            print(f'[ERROR1] {sym}: elements found but returned 0 rows... Trying one more time.')

            rows = get_rows(driver, sym)
            
            if len(rows) > 0:
                for row in rows:
                    parsed = scrape_row(row, sym)
                    
                    if len(parsed) > 0:
                        d.append(parsed)

                        if parsed['date'] == year_start:
                            break
            else:
                print(f'[ERROR2] {sym}: elements found but returned 0 rows... skipping stock.')
            
        t1 = time.time()
        t_total = round(t1 - t0, 2)
        print(f'{count}. Time: {t_total}s. Stock: {sym}')
        count += 1

        if t_total < 4:
            time.sleep(4 - t_total)

    driver.quit()

    return d