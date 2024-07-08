import psycopg2
import configparser


def connect_to_db():
    config = configparser.ConfigParser()
    config.read('upstream/config.ini')
    conn = psycopg2.connect(
        dbname=config['postgresql']['dbname'],
        user=config['postgresql']['user'],
        password=config['postgresql']['password'],
        host=config['postgresql']['host'],
        port=config['postgresql']['port']
    )
    return conn
