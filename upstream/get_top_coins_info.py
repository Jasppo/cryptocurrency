import configparser
from pycoingecko import CoinGeckoAPI
import psycopg2
from psycopg2 import sql
from functions.connect_to_db import connect_to_db

def get_top_coins_info():
    cg = CoinGeckoAPI() # Create an instance

    # Get the top 100 coins by market cap
    top_100_coins = cg.get_coins_markets(vs_currency='usd', order='market_cap_desc', per_page=100, page=1)

    # Extract relevant info: coin_id, ticker, name, image url
    coins = [(coin['id'], coin['symbol'], coin['name'], coin['image']) for coin in top_100_coins]

    return coins

def insert_coins_into_db(coins):
    conn = connect_to_db()
    cur = conn.cursor()

    insert_query = sql.SQL("""
                           INSERT INTO top_coins_info (cg_id, ticker, name, image_url) VALUES (%s, %s, %s, %s)
                           """
                           )
    
    for coin in coins:
        cur.execute(insert_query, coin)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    top_coins_info = get_top_coins_info()
    insert_coins_into_db(top_coins_info)
    print("Inserted top coins into the database")

