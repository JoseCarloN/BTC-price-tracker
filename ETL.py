from pymongo import MongoClient
import requests
import datetime as dt

# Binance API end point for getting candlestick bars data for a symbol (BTC/USD)
end_point = 'https://api.binance.com/api/v3/uiKlines'
parameters = {
    'symbol': 'BTCUSDT',
    'interval': "1d",  # data interval
    'timeZone': -6,  # converts from UTC response to Mexico timezone
    'limit': 1  # limit query to get only the most up-to-date data
}

response = requests.get(url=end_point, params=parameters)
response.raise_for_status()  # In case something goes wrong, try catch...
raw_data = response.json()  # Parse response's contents in json format

transformed_data = []
for daily_data in raw_data:
    open_timestamp = float(str(daily_data[0])[0:10])
    close_timestamp = float(str(daily_data[6])[0:10])

    temp_dict = {
        'open_datetime': dt.datetime.fromtimestamp(open_timestamp),
        'close_datetime': dt.datetime.fromtimestamp(close_timestamp),
        'open_price': daily_data[1],
        'high_price': daily_data[2],
        'low_price': daily_data[3],
        'close_price': daily_data[4]
    }
    transformed_data.append(temp_dict)  # this will be loaded to mongoDB cluster

# Initializing mongoDB client
uri = 'mongodb+srv://josephnavarrete2:oy6RPUMZLt8F4Kj7@btc-price-tracker.e3ejmb0.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(host=uri)
# Accessing/creating BTC database
db = client['BTC']
# Accessing/creating candlestick collection
collection = db['candlestick']
# Pushing transformed data to collection
collection.insert_one(transformed_data)