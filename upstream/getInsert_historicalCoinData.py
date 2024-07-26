import time
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta, timezone
import requests
import sys
from google.cloud import bigquery
import configparser
from functions import pull_data, get_bigquery_id

def connect_to_db():
    # Create a BigQuery client
    client = bigquery.Client()
    return client

def top_coins(client, dataset_id):
    query = f"""
    SELECT cg_id
    FROM `{dataset_id}.crypto.top_coins_info`
    ORDER BY cg_id
    LIMIT 100;
    """
    query_job = client.query(query)  # API request
    results = query_job.result()  # Waits for the query to finish
    return [row.cg_id for row in results]  # Return a list of cg_id

def insert_data(client, cg_id, data):
    table_id = 'crypto.coin_data'
    table = client.get_table(table_id)  # Make an API request.

    rows_to_insert = []

    # Get today's date in UTC
    today_utc = datetime.now(timezone.utc).date()

    for i, (timestamp, price) in enumerate(data['prices']):
        date = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)

        # Midnight check
        if date.time() == datetime.min.time():
            date -= timedelta(days=1)
        date = date.date()

        # Skip if the date is today's date in UTC
        if date == today_utc:
            continue

        # Format date as string in the format YYYY-MM-DD
        date_str = date.isoformat()

        price = round(price, 9)  # Ensuring price fits within NUMERIC scale
        volume = round(data['total_volumes'][i][1], 9)
        market_cap = round(data['market_caps'][i][1], 9)

        rows_to_insert.append({
            "cg_id": cg_id,  
            "date": date_str,
            "price": price,
            "volume": volume,
            "market_cap": market_cap
        })

    errors = client.insert_rows_json(table, rows_to_insert)  # API request
    if errors == []:
        print(f"Inserted data for CG ID {cg_id}")
    else:
        print(f"Errors occurred while inserting data for CG ID {cg_id}: {errors}")

def main():
    client = connect_to_db()
    dataset_id = get_bigquery_id()
    cg_ids = top_coins(client, dataset_id) 

    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=364)

    for cg_id in cg_ids:
        print(f"Fetching data for {cg_id}...")
        historical_data = pull_data(cg_id=cg_id, start_date=start_date, end_date=end_date, recent=False)
        insert_data(client=client, cg_id=cg_id, data=historical_data)  
        time.sleep(1)  # Add a short delay between requests

if __name__ == "__main__":
    main()