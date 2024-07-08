from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta
import time
import requests

def pull_data(cg_id, start_date, end_date, recent = True):
    cg = CoinGeckoAPI()

    retries = 5
    for i in range(retries):
        try:
            if recent:
                delta = end_date - start_date
                num_days = delta.days - 1
                data = cg.get_coin_market_chart_by_id(
                    id = cg_id,
                    vs_currency = 'usd',
                    days = num_days,
                    interval = 'daily')
            else:
                from_timestamp = int(start_date.timestamp())
                to_timestamp = int(end_date.timestamp())
                data = cg.get_coin_market_chart_range_by_id(
                cg_id, 
                vs_currency='usd', 
                from_timestamp=from_timestamp, 
                to_timestamp=to_timestamp)
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("Rate limit exceeded. Retrying in 60 seconds...")
                time.sleep(60)
            else:
                raise e
    raise Exception("Failed to fetch data after several retries.")