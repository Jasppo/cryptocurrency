from pycoingecko import CoinGeckoAPI
from google.cloud import bigquery
import sys

def get_top_coins_info():
    cg = CoinGeckoAPI() # Create an instance

    # Get the top 100 coins by market cap
    top_100_coins = cg.get_coins_markets(vs_currency='usd', order='market_cap_desc', per_page=100, page=1)

    # Extract relevant info: coin_id, ticker, name, image url
    coins = [(coin['id'], coin['symbol'], coin['name'], coin['image']) for coin in top_100_coins]

    return coins

def insert_coins_into_db(coins):
    client = bigquery.Client()

    dataset_id = 'crypto'
    table_id = 'top_coins_info'
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)  # Make an API request to fetch the table

    rows_to_insert = [
        {u"cg_id": coin[0], u"ticker": coin[1], u"name": coin[2], u"image_url": coin[3]} for coin in coins
    ]

    errors = client.insert_rows_json(table, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
        sys.exit(1)

if __name__ == "__main__":
    top_coins_info = get_top_coins_info()
    insert_coins_into_db(top_coins_info)
    print("Inserted top coins into the database")