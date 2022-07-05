import pandas as pd
import numpy as np
import requests
import multitasking
import datetime
#from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()
import pymongo as pym
from pymongo import MongoClient
from yahoofinancials import YahooFinancials


cluster = 'mongodb+srv://?????@cluster0.86nym.mongodb.net/test?retryWrites=true&w=majority'
client = MongoClient(cluster)
print(client.list_database_names())
### Accessing a database:
db = client.test
col = db.data
print(db.list_collection_names())
print(db["data"])
print(col)
### Count times of TSLA apperence in database:
isthere = col.count_documents({"symbol": "TSLA"})
print(isthere)
### Print all documents
allmydocs = col.find()
for x in allmydocs:
        print(x)
print("********")
### Print the last item (newest) and than print all documents in descending order
mydoc = col.find().sort("EndDate", -1)
mylast = col.find_one()
retrievedate = col.find_one()["EndDate"]
print(mylast)
print(retrievedate)
print("*****")
for x in mydoc:
  print(x)


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
#result = data.find_one()
#print(result)
## To retrive specific data:
#res = data.find_one({"symbol": "APPL"})
#print(res)
## To sort according to end date:
#re = data.find().sort({"EndDate": -1})
#print(re)


symbol = 'TSLA'
ticker = yf.Ticker(symbol)
#print(ticker.history())
end_time = datetime.datetime.now()
start_time = end_time - datetime.timedelta(days=365)
tickerdata = ticker.history(start=retrievedate, end=end_time)
print('*')
print(tickerdata)
print('*')
### The schema according to which new document are being build:
dataoflists = {'Date': [datetime.datetime.now()], 'Open': [0], 'High': [1], 'Low': [1], 'Close': [1], 'AdjClose': [1], 'Volume': [1], 'Project_id': [symbol]}
### Inserting a new document following every search of symbol by client:
x = col.insert_one(dataoflists)
df = pd.DataFrame(dataoflists)
print(df)

#print(tickerdata)
#print(end_time)
#print(start_time)
#temp = datetime.timedelta('start_time', 'end_time')
# df = pd.DataFrame({"tickerdata"})
# df
#temp = pd.DataFrame.between_time('start_time', 'end_time')
#print(temp)
#filtering for last date in sort results:
#firstquary = db.data.findOne({'EndDate'})
for x in db.data.find():
    if db.data.count_documents({"symbol": "TSLA"})==0:
#if db.data.count_documents({'symbol': "TSLA"}, limit=1) == 0:
        print("DataFrame.empty")
else:
    print(ticker.history(start=retrievedate, end=end_time))





#else:data.find().sort("EndDate", -1)
## retrieve between_dates()
# symbol_df = yf.download('symbol',
#                       start='2019-01-01',
#                       end=firstquary,
#                       progress=False,)
# print(symbol_df.head())\]
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