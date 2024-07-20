CREATE TABLE IF NOT EXISTS pchange (
    coin_id INTEGER,
    date DATE,
    pct_change_1_day FLOAT,
    pct_change_7_days FLOAT,
    pct_change_14_days FLOAT,
    pct_change_30_days FLOAT,
    pct_change_90_days FLOAT,
    pct_change_180_days FLOAT,
    pct_change_365_days FLOAT,
    PRIMARY KEY (coin_id),
    FOREIGN KEY (coin_id) REFERENCES top_coins_info(id)
);