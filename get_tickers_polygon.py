
from polygon import RESTClient
import config
client = RESTClient(config.API_KEY)
from tickers import tickersv4


counter = 0 

activeTickers = []

for t in client.list_tickers(market='stocks',limit=1000):

    activeTickers.append(t.ticker)

    counter += 1
    # print(counter)

# print(activeTickers)
commonTickers = [ticker for ticker in tickersv4 if ticker in activeTickers]

print(f"Fetched {len(commonTickers)} active tickers")
print("The common tickers are" ,commonTickers)