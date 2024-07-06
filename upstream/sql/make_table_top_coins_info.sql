CREATE TABLE top_coins_info (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL
);