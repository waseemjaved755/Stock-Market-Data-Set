from polygon import RESTClient
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import config
import time

# Initialize Polygon client
client = RESTClient(config.API_KEY)

DATABASE_URL = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'
engine = create_engine(DATABASE_URL)
connection = engine.connect()

query = "SELECT ticker FROM tickerstable WHERE status = true "
ticker_data_from_db = pd.read_sql(query, connection)

print("Ticker Data", ticker_data_from_db)

data = pd.DataFrame(columns=['ticker', 'open', 'high', 'low', 'close', 'volume', 'vwap', 'timestamp'])

print("Printing data to insert", data)

for i, row in ticker_data_from_db.iterrows():
    print("THIS IS THE ROW", row)
    ticker = row['ticker']
    print("This is the ticker", ticker)
    polygon_response = client.get_aggs(ticker=ticker, multiplier=1, timespan='day', from_='2024-07-24', to='2024-07-24')
    print("Getting the response from polygon.io", polygon_response)

    for result in polygon_response:
        date = datetime.utcfromtimestamp(result.timestamp / 1000).date()
        tickerdate = date
        new_row = {
            'ticker': ticker,
            'open': result.open,
            'high': result.high,
            'low': result.low,
            'close': result.close,
            'volume': result.volume,
            'vwap': result.vwap,
            'timestamp': tickerdate
        }
        data = pd.concat([data, pd.DataFrame([new_row])], ignore_index=True)

print("This is the data to insert into the db")
print("------------------------------------")
print(data)

data['volume'] = data['volume'].astype('int64')
print("This is the data type of the insertion column of volumn" ,data.dtypes)


start_time = time.time()  # get start time before insert

# Create a temporary table
temp_table = 'polygondata_temp'
data.to_sql(temp_table, con=engine, if_exists='replace', index=False)

# Update existing rows and insert new rows
update_query = f"""
-- Update existing rows
UPDATE polygondata
SET
    open = temp.open,
    high = temp.high,
    low = temp.low,
    close = temp.close,
    volume = temp.volume,
    vwap = temp.vwap,
    timestamp = temp.timestamp
FROM {temp_table} AS temp
WHERE polygondata.ticker = temp.ticker;
"""

try:
    connection.execute(text(update_query))
    connection.commit()  # Ensure the changes are committed
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Insert and update time: {total_time} seconds")
    print(data.head())
    print("DataFrame shape:", data.shape)
    print("DataFrame info:")
    print(data.info())

    print("Data Inserted/Updated")
except Exception as e:
    connection.rollback()  # Rollback the transaction on error
    print("An error occurred during the upsert operation:", e)

connection.close()
