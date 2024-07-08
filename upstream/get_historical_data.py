import time
import psycopg2
import configparser
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta
import requests
from functions.connect_to_db import connect_to_db
from functions.import_top_coins import top_coins
from functions.insert_coin_data import insert_data
from functions.pull_data import pull_data

def main():
    conn = connect_to_db()
    coins = top_coins(conn)

    end_date = datetime.now() - timedelta(days = 1)
    start_date = end_date - timedelta(days=364)

    for coin in coins:
        key_id, cg_id = coin
        print(f"Fetching data for {cg_id} (ID: {key_id})...")
        historical_data = pull_data(cg_id=cg_id, start_date=start_date, end_date=end_date, recent=False)
        insert_data(conn=conn, key_id=key_id, data=historical_data)
        time.sleep(1) # Add a short delay between requests

    conn.close()

if __name__ == "__main__":
    main()

