CREATE TABLE top_coins_info (
    id SERIAL PRIMARY KEY,
    cg_id VARCHAR(100) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    image_url TEXT
);