CREATE TABLE coin_data (
    id SERIAL PRIMARY KEY,
    coin_id INTEGER REFERENCES top_coins_info(id),
    date DATE NOT NULL,
    price NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    market_cap NUMERIC NOT NULL
);