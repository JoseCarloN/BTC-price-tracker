from pymongo import MongoClient
import requests
import config

# Binance API end point for getting candlestick bars data for a symbol (BTC/USD)
end_point = 'https://api.binance.com/api/v3/uiKlines'
parameters = {
    'symbol': 'BTCUSDT',
    'interval': "1d",  # data interval
    'timeZone': -6,  # converts from UTC response to Mexico timezone
    'limit': 1000  # limit query to get only the most up-to-date data
}

response = requests.get(url=end_point, params=parameters)
response.raise_for_status()  # In case something goes wrong, try catch...
raw_data = response.json()  # Parse response's contents in json format

transformed_data = []
for daily_data in raw_data:
    temp_dict = {
        'date': daily_data[0],
        'open': daily_data[1],
        'high': daily_data[2],
        'low': daily_data[3],
        'close': daily_data[4],
        'volume': daily_data[5]
    }
    transformed_data.append(temp_dict)  # this will be loaded to mongoDB cluster

# Initializing mongoDB client
uri = f'mongodb+srv://josephnavarrete2:{config.MDB_PSWD}@btc-price-tracker.e3ejmb0.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(host=uri)
# Accessing/creating BTC database
db = client['BTC']
# Accessing/creating candlestick collection
collection = db['candlestick']
# Pushing transformed data to collection
collection.insert_many(transformed_data)