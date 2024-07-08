import psycopg2
import time
from datetime import datetime, timedelta, timezone

def insert_data(conn, key_id, data):
    cur = conn.cursor()

    # Extracting relevant data from historical_data
    prices = data.get('prices', [])
    volumes = data.get('total_volumes', [])
    market_caps = data.get('market_caps', [])

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

        volume = data['total_volumes'][i][1]
        market_cap = data['market_caps'][i][1]

        # Insert data into coin_historical_data table
        insert_query = """
        INSERT INTO coin_data (coin_id, date, price, volume, market_cap)
        VALUES (%s, %s, %s, %s, %s);
        """
        print(f"Inserting data: Date is {date}")
        cur.execute(insert_query, (key_id, date, price, volume, market_cap))

    conn.commit()
    cur.close()