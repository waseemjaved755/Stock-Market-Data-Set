import requests

from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage
from typing import List
from tickers import tickersv3 , tickersv4
import config
from datetime import datetime, timezone
import sys
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime
from polygon import RESTClient


host = 'http://127.0.0.1:9000'

# Function to run a SQL query
def run_query(sql_query):
    
    query_params = {'query': sql_query, 'fmt' : 'json'}
    try:
        response = requests.get(host + '/exec', params=query_params)
        json_response = json.loads(response.text)
        print(json_response)
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}', file=sys.stderr)

def handle_msg(msg: List[WebSocketMessage]):
    for m in msg:
        print(f"""
                Symbol: {m.symbol}
                Volume: {m.volume}
                Accumulated Volume: {m.accumulated_volume}
                Official Open Price: {m.official_open_price}
                VWAP: {m.vwap}
                Open: {m.open}
                Close: {m.close}
                High: {m.high}
                Low: {m.low}
                Aggregate VWAP: {m.aggregate_vwap}
                Average Size: {m.average_size}
                Start Timestamp: {m.start_timestamp}
                End Timestamp: {m.end_timestamp}
                OTC: {m.otc}
                """)
        data = {
            "symbol": m.symbol,
            "volume": m.volume,
            "accumulated_volume": m.accumulated_volume,
            "official_open_price": m.official_open_price,
            "vwap": m.vwap,
            "open": m.open,
            "close": m.close,
            "high": m.high,
            "low": m.low,
            "aggregate_vwap": m.aggregate_vwap,
            "average_size": m.average_size,
            "start_timestamp": m.start_timestamp,
            "end_timestamp": m.end_timestamp,
            "otc": m.otc
        }
        insert_tickers_to_questdb(data)



# Function to insert data into QuestDB
def insert_tickers_to_questdb(data):
    print("Coming to insertion part")

    symbol = data["symbol"]
    volume = data["volume"]
    accumulated_volume = data["accumulated_volume"]
    official_open_price = data["official_open_price"]
    vwap = data["vwap"]
    open_price = data["open"]
    close_price = data["close"]
    high_price = data["high"]
    low_price = data["low"]
    aggregate_vwap = data["aggregate_vwap"]
    average_size = data["average_size"]
    start_timestamp = data["start_timestamp"]

    timestamp = datetime.fromtimestamp(start_timestamp / 1000, tz=timezone.utc).isoformat()
        
    sql_query = f"INSERT INTO TickersAggregateDataV3 VALUES ('{symbol}', '{volume}', '{accumulated_volume}', '{official_open_price}', '{vwap}', '{open_price}', '{close_price}', '{high_price}', '{low_price}', '{aggregate_vwap}', '{average_size}','{timestamp}')"
    run_query(sql_query)


def main():
    no_data = []
    inactive_tickers = []
    try:
        ws = WebSocketClient(config.API_KEY)
        
        for ticker in tickersv3:
            ws.subscribe(f"A.{ticker}")

        print(f"Subscribed to {len(ticker)} tickers")
        ws.run(handle_msg)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
