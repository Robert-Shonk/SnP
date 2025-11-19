DROP TABLE IF EXISTS snp;

CREATE TABLE snp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchangeUrl TEXT NOT NULL,
    symbol TEXT NOT NULL,
    security TEXT NOT NULL,
    sector TEXT NOT NULL,
    subIndustry TEXT NOT NULL,
    hqLocation TEXT NOT NULL,
    dateAdded TEXT NOT NULL,
    CIK INTEGER NOT NULL,
    founded TEXT NOT NULL -- text since years presented as "2013 (1888)"
);