
CREATE TABLE tickersTable (
    ticker VARCHAR PRIMARY KEY,
    status BOOLEAN,
    minute_data BOOLEAN,
    options_chain BOOLEAN
);

CREATE TABLE polygondata (
    
    ticker VARCHAR REFERENCES tickerstable(ticker),
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    vwap DOUBLE precision,
    timestamp TIMESTAMPTZ,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
);
ALTER TABLE polygondata 
ALTER COLUMN volume TYPE double precision USING volume::double precision;


select * from polygondata where ticker ='ETRN'
DELETE FROM tickerstable
WHERE ticker = 'ETRN';


SELECT t.ticker
FROM tickerstable t
LEFT JOIN polygondata pd ON t.ticker = pd.ticker
WHERE pd.ticker IS NULL;



 
