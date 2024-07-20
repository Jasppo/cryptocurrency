import psycopg2
import configparser
import pandas as pd
from datetime import timedelta
from functions.connect_to_db import connect_to_db

def fetch_recent_and_historical_prices():
    conn = connect_to_db()
    query = """
    WITH PriceData AS (
        SELECT 
            coin_id, 
            date, 
            price,
            LEAD(price, 1) OVER (PARTITION BY coin_id ORDER BY date DESC) AS price_1_day_ago,
            LEAD(price, 7) OVER (PARTITION BY coin_id ORDER BY date DESC) AS price_7_days_ago,
            LEAD(price, 14) OVER (PARTITION BY coin_id ORDER BY date DESC) AS price_14_days_ago,
            LEAD(price, 30) OVER (PARTITION BY coin_id ORDER BY date DESC) AS price_30_days_ago,
            LEAD(price, 90) OVER (PARTITION BY coin_id ORDER BY date DESC) AS price_90_days_ago,
            LEAD(price, 180) OVER (PARTITION BY coin_id ORDER BY date DESC) AS price_180_days_ago,
            LEAD(price, 365) OVER (PARTITION BY coin_id ORDER BY date DESC) AS price_365_days_ago
        FROM coin_data
        WHERE date >= (SELECT MAX(date) - INTERVAL '366 days' FROM coin_data WHERE coin_id = coin_data.coin_id)
    )
    SELECT *
    FROM PriceData
    WHERE date = (SELECT MAX(date) FROM PriceData WHERE coin_id = PriceData.coin_id);
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

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
    return df

def prepare_data_for_insert(df):
    columns = ['coin_id', 'date'] + [f'pct_change_{days}_days' for days in [1, 7, 14, 30, 90, 180, 365]]
    return df[columns]

def insert_data(df):
    conn = connect_to_db()
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
        INSERT INTO pChange (coin_id, date, pct_change_1_day, pct_change_7_days, pct_change_14_days, pct_change_30_days, pct_change_90_days, pct_change_180_days, pct_change_365_days)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (coin_id) DO UPDATE SET
        date = EXCLUDED.date,
        pct_change_1_day = EXCLUDED.pct_change_1_day,
        pct_change_7_days = EXCLUDED.pct_change_7_days,
        pct_change_14_days = EXCLUDED.pct_change_14_days,
        pct_change_30_days = EXCLUDED.pct_change_30_days,
        pct_change_90_days = EXCLUDED.pct_change_90_days,
        pct_change_180_days = EXCLUDED.pct_change_180_days,
        pct_change_365_days = EXCLUDED.pct_change_365_days;
        """, tuple(row))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    df = fetch_recent_and_historical_prices()
    df = compute_percentage_changes(df)
    df = prepare_data_for_insert(df)
    insert_data(df)

if __name__ == "__main__":
    main()