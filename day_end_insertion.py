import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import config

def end_of_day_update():
    DATABASE_URL = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'
    engine = create_engine(DATABASE_URL)
    connection = engine.connect()

    # Select final data from the temporary table
    query = "SELECT * FROM polygondata_temp"
    temp_data = pd.read_sql(query, connection)


    print("This is temp Data" , temp_data)
    print("Data types of columns:")
    print(temp_data.dtypes)
    temp_data['volume'] = pd.to_numeric(temp_data['volume'], errors='coerce').fillna(0).astype('int64')
    print("Data types after conversion:")
    print(temp_data.dtypes)


    # Move data to the main data table
    try:
        temp_data.to_sql('polygondata', con=engine, if_exists='append', index=False)
        print("Data moved to polygondata our main table")

    except Exception as e:
        print("An error occurred during end-of-day update", e)
    finally:
        connection.close()

if __name__ == '__main__':
    end_of_day_update()
