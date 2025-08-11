import urls

import requests
from bs4 import BeautifulSoup
import pandas as pd

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
            
            data['exchange_url'].append(tds[0].find('a')['href'])
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