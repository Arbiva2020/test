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


cluster = 'mongodb+srv://arbiva:Adi101010@cluster0.86nym.mongodb.net/test?retryWrites=true&w=majority'
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

print("********")


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
#print('*')
#print(tickerdata)
#print('*')
### The schema according to which new document are being build, that import data from yf:
# dataoflists = {
#                'Date': datetime.datetime.utcnow(),
#                'Open': [ticker.history(start="2022-07-01", end=datetime.datetime.utcnow(), frequency='1dy')['Open']],
#                'High': [ticker.history(start="2022-07-01", end=datetime.datetime.utcnow(), frequency='1dy')['High']],
#                'Low': [ticker.history(start="2022-07-01", end=datetime.datetime.utcnow(), frequency='1dy')['Low']],
#                'Close': [ticker.history(start="2022-07-01", end=datetime.datetime.utcnow(), frequency='1dy')['Close']],
#                #'AdjClose': [ticker.history(start="2022-07-01", end=datetime.datetime.utcnow(), frequency='1dy')['AdjClose']],
#                'Volume': [ticker.history(start="2022-07-01", end=datetime.datetime.utcnow(), frequency='1dy')['Volume']],
#                'Project_id': symbol
#                }

dataofnew = {
    'StartDate': (end_time)-datetime.timedelta(days=365),
    'EndDate': (end_time),
    'Open': [1],
    'High': [2],
    'Low': [3],
    'Close': [4],
    'AdjClose': [5],
    'Volume': [6],
    'Project_id': (symbol)
}

dataoflists = {
    'StartDate': mylast["EndDate"],
    'EndDate': (end_time),
    'Open': (ticker.history(start=retrievedate, end=end_time)["Open"]),
    'High': 2,
    'Low': 3,
    'Close': 4,
    'AdjClose': 5,
    'Volume': 6,
    'Project_id': (symbol)
}


### Inserting a new document following every search of symbol by client:
#x = col.insert_one(dataoflists)
#df = pd.DataFrame(dataoflists)
#print(df)

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
        ## creation of object of **new** symbol in our mongodb:
        x = col.insert_one(dataofnew)
        dn = pd.DataFrame(dataofnew)
        print(dn)
        print("DataFrame.empty")
else:
    ## creation of additional object of **excisting** symbol in our mongodb:
    print('### what we store in the db:')
    x = col.insert_one(dataoflists)
    df = pd.DataFrame(dataoflists)
    print(df)
    ## print of output
    print('### what we get straight from yf according to relevant dates:')
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