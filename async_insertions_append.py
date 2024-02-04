import polygon
import pandas as pd
import pytz
from sqlalchemy import create_engine, text
from datetime import datetime
import config
import time



async def main():

    stocks_client = polygon.StocksClient('RIpPfAi9K4XH0J5uK1lHchquRUg_91CG', True)
    DATABASE_URL = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'
    engine = create_engine(DATABASE_URL)
    connection = engine.connect()

    query = "SELECT ticker FROM tickerstable WHERE status = true "

    ticker_data = pd.read_sql(query, connection)
    data_to_insert = pd.DataFrame(columns=['ticker', 'open', 'high', 'low', 'close', 'volume' , 'vwap' , 'timestamp'])
    start_time = time.time()
    print("Ticker Data" , ticker_data)
    for _ , row in ticker_data.iterrows():
        ticker = row['ticker']
        resp = await stocks_client.get_aggregate_bars(symbol=ticker, multiplier=1, timespan='day', from_date='2024-06-03', to_date='2024-06-05')
        if 'results' in resp:
            for result in resp['results']:
                date = datetime.utcfromtimestamp(result['t'] / 1000).date()
                data_to_insert = data_to_insert._append({
                    'ticker': ticker,
                    'open': result['o'],
                    'high': result['h'],
                    'low': result['l'],
                    'close': result['c'],
                    'volume': result['v'],
                    'vwap': result['vw'],
                    'timestamp': date
                }, ignore_index=True)
                print("========================" ,data_to_insert)
    await stocks_client.close()
    end_time = time.time()
    total_time = (end_time - start_time)/60
    
    print("This is the data to insert dataframe" ,data_to_insert) 
    print(f"Helo Helo Time taken for the data frame: {total_time} minutes")

    try:
        data_to_insert.to_sql('polygondata', con=engine, if_exists='append', index=False)
        print("Data Inserted")
    except Exception as e:
        print("An error in insertion" , e)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())