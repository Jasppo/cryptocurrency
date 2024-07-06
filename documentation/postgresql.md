# List tables
\dt

# Drop tables
DROP TABLE IF EXISTS table_name;

# Create tables
CREATE TABLE table_name (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL
);

# Exit psql
\q

# Connecting to database securely in Python

## Create a configuration file - config.ini
[postgresql]
dbname = database_name
user = your_username
password = your_password
host = localhost
port = 5432

## Connect to database in Python
import configparser 
import psycopg2
from psycopg2 import sql

config = configparser.ConfigParser()
config.read('config.ini')

conn = psycopg2.connect(
        dbname=config['postgresql']['dbname'],
        user=config['postgresql']['user'],
        password=config['postgresql']['password'],
        host=config['postgresql']['host'],
        port=config['postgresql']['port']
    )
cur = conn.cursor()
...
cur.execute(...)
conn.commit()
cur.close()
conn.close()