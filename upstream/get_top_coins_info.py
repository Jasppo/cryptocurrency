import configparser
from pycoingecko import CoinGeckoAPI
import psycopg2
from psycopg2 import sql

def get_top_coins_info():
    cg = CoinGeckoAPI() # Create an instance

    # Get the top 100 coins by market cap
    top_100_coins = cg.get_coins_markets(vs_currency='usd', order='market_cap_desc', per_page=100, page=1)

    # Extract ticker and name
    coins = [(coin['symbol'], coin['name']) for coin in top_100_coins]

    return coins

def insert_coins_into_db(coins):
    config = configparser.ConfigParser()
    config.read('upstream/config.ini')

    conn = psycopg2.connect(
        dbname = config['postgresql']['dbname'],
        user = config['postgresql']['user'],
        password = config['postgresql']['password'],
        host = config['postgresql']['host'],
        port = config['postgresql']['port']
    )
    cur = conn.cursor()

    insert_query = sql.SQL("""
                           INSERT INTO top_coins_info (ticker, name) VALUES (%s, %s)
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

