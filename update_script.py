import scrape
import data_util
import urls
import Secrets

from datetime import datetime
import json

import pandas as pd
import requests

headers = {
    "x-api-key": Secrets.API_KEY,
    "Content-Type": "application/json"
}

today = datetime.today()
day_str = f"{today.year}-{today.month}-{today.day}"

new_snp = scrape.get_list()

results = {}
if new_snp != -1:
    new_stocks = new_snp["symbol"]

    stock_diff = data_util.compare_snp(new_stocks)

    if stock_diff != -1:
        # remove stocks that are in stock_diff["old_stocks"].
        data = json.dumps(stock_diff["old_stocks"])
        post_remove_old = requests.delete(urls.prod_delete_stocks_url, data=data, headers=headers)
        results["remove_old"] = post_remove_old.status_code

        # scrape year to date data of stocks in stock_diff["new_stocks"] then post to db for insert.
        new_ytd = scrape.get_data(stock_diff["new_stocks"])
        df = pd.DataFrame(new_ytd)
        new_with_moves = data_util.calc_moves(df)
        data = json.dumps(new_with_moves.to_dict(orient="records"))
        post_add_new = requests.post(urls.prod_insert_stocks_url, data=data, headers=headers)
        results["post_add_new"] = post_add_new.status_code

        # finally create list of stocks that have stayed on list and scrape their most recent day's data.
        remaining_new_stocks = []
        for stock in new_stocks:
            if stock not in stock_diff["new_stocks"]:
                remaining_new_stocks.append(stock)

        remaining_update = scrape.get_data(remaining_new_stocks, date_end=day_str)
        df = pd.DataFrame(remaining_update)
        remaining_with_moves = data_util.calc_moves(df)
        data = json.dumps(remaining_with_moves.to_dict(orient="records"))
        post_update = requests.post(urls.prod_insert_stocks_url, data=data, headers=headers)
        results["post_update"] = post_update.status_code

        # post new snp_list
        data = json.dumps(new_snp.to_dict(orient="records"))
        post_list = requests.post(urls.prod_replace_snplist, data=data, headers=headers)

        print("Update with new stocks successfull!")

    else: # if no new stocks just get daily data
        daily = scrape.get_data(new_stocks, date_end=day_str)
        df = pd.DataFrame(daily)
        daily_with_moves = data_util.calc_moves(df)
        data = json.dumps(daily_with_moves.to_dict(orient="records"))
        post_update = requests.post(urls.prod_insert_stocks_url, data=data, headers=headers)
        results["post_update"] = post_update.status_code

        print("Update with no new stocks successfull!")


    # get basic S&P500 data
    snp_basic = scrape.get_daily_data()
    if snp_basic != -1:
        snp_basic["date"] = day_str
        data = json.dumps(snp_basic)
        post_basic = requests.put(urls.prod_update_snp_daily_url, data=data, headers=headers)
        results["post_basic"] = post_basic.status_code
else:
    print("Error: scrape.get_ist() failed...")

print(results)