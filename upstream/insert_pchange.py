import pandas as pd
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from pycoingecko import CoinGeckoAPI
import sys
import configparser
from functions import get_bigquery_id

def connect_to_db():
    # Construct a BigQuery client object.
    return bigquery.Client()

def fetch_recent_and_historical_prices(client, dataset_id):
    query = f"""
    WITH PriceData AS (
        SELECT 
            cg_id, 
            date, 
            price,
            LEAD(price, 1) OVER (PARTITION BY cg_id ORDER BY date DESC) AS price_1_day_ago,
            LEAD(price, 7) OVER (PARTITION BY cg_id ORDER BY date DESC) AS price_7_days_ago,
            LEAD(price, 14) OVER (PARTITION BY cg_id ORDER BY date DESC) AS price_14_days_ago,
            LEAD(price, 30) OVER (PARTITION BY cg_id ORDER BY date DESC) AS price_30_days_ago,
            LEAD(price, 90) OVER (PARTITION BY cg_id ORDER BY date DESC) AS price_90_days_ago,
            LEAD(price, 180) OVER (PARTITION BY cg_id ORDER BY date DESC) AS price_180_days_ago,
            LEAD(price, 365) OVER (PARTITION BY cg_id ORDER BY date DESC) AS price_365_days_ago
        FROM `{dataset_id}.crypto.coin_data`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 366 DAY)
    )
    SELECT *
    FROM PriceData
    WHERE date = (SELECT MAX(date) FROM PriceData WHERE cg_id = PriceData.cg_id);
    """
    query_job = client.query(query)  # API request - starts the query
    return query_job.to_dataframe()  # Convert to DataFrame

def compute_percentage_changes(df):
    periods = [1, 7, 14, 30, 90, 180, 365]
    for days in periods:
        if days == 1:
            column_name = 'price_1_day_ago'  # Specific handling for the singular case
        else:
            column_name = f'price_{days}_days_ago'
            
        df[f'pct_change_{days}_days'] = df.apply(
            lambda row: ((row['price'] - row[column_name]) / row[column_name] * 100)
            if row[column_name] is not None and row['price'] is not None and row[column_name] != 0
            else None,
            axis=1
        )
    df = df.rename(columns={'pct_change_1_days': 'pct_change_1_day'})
    price_columns = ['price', 'price_1_day_ago', 'price_7_days_ago', 'price_14_days_ago', 
                     'price_30_days_ago', 'price_90_days_ago', 'price_180_days_ago', 'price_365_days_ago']
    return df.drop(columns=price_columns, errors='ignore')  # errors='ignore' will prevent error if a column is missing

def pivot_long(df):
    df_long = df.melt(id_vars=['cg_id', 'date'], 
                      var_name='period', 
                      value_name='perc',
                      value_vars=['pct_change_1_day', 'pct_change_7_days', 'pct_change_14_days', 'pct_change_30_days', 
                                  'pct_change_90_days', 'pct_change_180_days', 'pct_change_365_days'])

    # Rename the period names to more descriptive ones if necessary
    period_mapping = {
        'pct_change_1_day': '1 day',
        'pct_change_7_days': '7 days',
        'pct_change_14_days': '14 days',
        'pct_change_30_days': '30 days',
        'pct_change_90_days': '90 days',
        'pct_change_180_days': '180 days',
        'pct_change_365_days': '365 days'
    }
    df_long['period'] = df_long['period'].map(period_mapping)
    return df_long

def insert_data(client, df, df_long):
    dataset_id = 'crypto'
    table_id = f'{dataset_id}.pchange'
    table_id_long = f'{dataset_id}.pchange_long'

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #Overwrite table

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    job_long = client.load_table_from_dataframe(df_long, table_id_long, job_config=job_config)
    job_long.result()  # Wait for the job to complete

    if not job.errors:
        print(f"Data inserted successfully into {table_id}")
    else:
        print(f"Errors occurred: {job.errors}")

    if not job_long.errors:
        print(f"Data inserted successfully into {table_id_long}")
    else:
        print(f"Errors occurred: {job_long.errors}")

def main():
    client = connect_to_db()
    dataset_id = get_bigquery_id()
    df = fetch_recent_and_historical_prices(client, dataset_id)
    df = compute_percentage_changes(df)
    df_long = pivot_long(df)
    insert_data(client, df, df_long)

if __name__ == "__main__":
    main()