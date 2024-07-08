import time
import psycopg2
import configparser
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta, timezone
import requests
from functions.connect_to_db import connect_to_db
from functions.import_top_coins import top_coins
from functions.insert_coin_data import insert_data
from functions.pull_data import pull_data

def main():
    conn = connect_to_db()
    coins = top_coins(conn)

    for coin in coins:
        key_id, cg_id = coin

        # Get most recent date in database
        cur = conn.cursor()
        query = """
        SELECT date 
        FROM coin_data 
        WHERE coin_id = %s 
        ORDER BY date DESC 
        LIMIT 1;
        """
        cur.execute(query, (key_id,))
        most_recent_date = cur.fetchone()[0]
        cur.close()

        # 7 hour difference between PST and UTC; Script is ran at 2 AM UTC, or 7 PM PST daily
        # Get today's date in UTC
        today_utc = datetime.now(timezone.utc).date()
        print(f"Fetching data for {cg_id} (ID: {key_id})...")
        historical_data = pull_data(cg_id=cg_id, start_date=most_recent_date, end_date=today_utc, recent=True)
        insert_data(conn=conn, key_id=key_id, data=historical_data)
        time.sleep(1) # Add a short delay between requests

    conn.close()

if __name__ == "__main__":
    main()