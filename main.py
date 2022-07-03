import pandas as pd
import numpy as np
import requests
import multitasking
import datetime
from pandas_datareader import data as pdr
import yfinance as yf
import pymongo as pym
from pymongo import MongoClient
from yahoofinancials import YahooFinancials


cluster = 'mongodb+srv://arbiva:Adi101010@cluster0.86nym.mongodb.net/test?retryWrites=true&w=majority'
client = MongoClient(cluster)
print(client.list_database_names())
### Accessing a database:
db = client.test
print(db.list_collection_names())
### Creating hard code demo data:
# dat1 = {"symbol": "TSLA", "StartDate": "2020-01-01", "EndDate": datetime.datetime .utcnow()}

### Accessing the collection by name:
data = db.data

### In order to insert our mock data into the collection:
# result = data.insert_one(dat1)

### Creating hard code demo data - more tham one object:
# dat2 = [{"symbol": "TSLA", "StartDate": "2020-01-01", "EndDate": datetime.datetime.utcnow()},
#         {"symbol": "APPL", "StartDate": "2021-01-01", "EndDate": datetime.datetime.utcnow()}]

# result = data.insert_many(dat2)

### In order to retrive data, we implement find metods:
## To get the firs match:
result = data.find_one()
print(result)
## To retrive specific data:
# result = data.find_one({"symbol": "TSLA"})


symbol = 'TSLA'
ticker = yf.Ticker(symbol)
print(ticker.history())
end_time = datetime.datetime.now()
start_time = end_time - datetime.timedelta(days=365)
tickerdata = ticker.history(start=start_time, end=end_time)
print(tickerdata)
print(end_time)
print(start_time)
# df = pd.DataFrame({"tickerdata"})
# df
temp = pd.DataFrame.between_time('start_time', 'end_time')
print(temp)
#filtering for last date in sort results:
firstquary = db.data.findOne({'EndDate'})
if db.data.count_documents({'symbol': "TSLA"}, limit=1) == 0:
        print(ticker.history(temp))
else:data.find().sort("EndDate", -1)
## retrieve between_dates()
# symbol_df = yf.download('symbol',
#                       start='2019-01-01',
#                       end=firstquary,
#                       progress=False,)
# print(symbol_df.head())



### Creating a database
# mydb = client["test"]
#creating a collection
# data_table = mydb["data"]

#If this is the first quary for this specific symbol:
# end = dt.datetime.now()
# start = end - dt.timedelta(days=365)
# start, end




#Creating a Ticker obgect - this object suppose to get what the client is inputing as 'SYMBOL'
# symbol = 'TSLA'
# ticker = yf.Ticker(symbol)
#When we whant to view stock history (can get parameters as "max" and so on)
# print(ticker.history())


# database.create_collection("data")
# client = pymongo.MongoClient('mongodb://localhost:27017')

# tsla_df = yf.download('TSLA',
#                       start='2019-01-01',
#                       end='2021-06-12',
#                       progress=False,
# )
# print(tsla_df.head())