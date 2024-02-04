
import pandas as pd
from sqlalchemy import create_engine, Boolean
from tickers import tickersv4

DATABASE_URL = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'
engine = create_engine(DATABASE_URL)

try:
    with engine.connect() as connection:
        print("Connection established")

        data = {'ticker': tickersv4, 'status': [True] * len(tickersv4)}  
        df = pd.DataFrame(data)

        print("DataFrame created:")
        print(df.head())

      
        df.to_sql(
            'tickerstable',
            con=connection,
            if_exists='append',  # Use 'replace' if you want to drop and recreate the table
            index=False,
            dtype={'status': Boolean()}  
        )
        print("Data inserted successfully")

except Exception as e:
    print("An error occurred:", e)
finally:
    engine.dispose()  
    print("Connection closed")





