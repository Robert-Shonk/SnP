CREATE TABLE snp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange_url TEXT NOT NULL,
    symbol TEXT NOT NULL,
    security TEXT NOT NULL,
    sector TEXT NOT NULL,
    sub_industry TEXT NOT NULL,
    hq_location TEXT NOT NULL,
    date_added TEXT NOT NULL,
    CIK INTEGER NOT NULL,
    founded TEXT NOT NULL -- text since years presented as "2013 (1888)"
);