import pandas as pd
import numpy as np
import requests
import multitasking
import datetime
import pandas_datareader as dr
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()
import pymongo as pym
from pymongo import MongoClient
from yahoofinancials import YahooFinancials


########################## Working with mongodb:################################
cluster = 'mongodb+srv://arbiva:?????@cluster0.86nym.mongodb.net/test?retryWrites=true&w=majority'
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
### Accessing the collection by name:
data = db.data


########################## Working with yf and tickers:################################
symbol = 'TSLA'
ticker = yf.Ticker(symbol)
#print(ticker.history())
end_time = datetime.datetime.now()
start_time = end_time - datetime.timedelta(days=365)
tickerdata = ticker.history(start=retrievedate, end=end_time)

########################## converting pdr to df:################################
print('*')
#df = dr.data.get_data_yahoo({symbol}, start=start_time, end=end_time)
#df
data1 = pdr.get_data_yahoo(symbol, start=start_time, end=end_time)  #for new queries, a year back
data2 = pdr.get_data_yahoo(symbol, start=retrievedate, end=end_time)  #for existing symbols, starting last search ("end date")
print(data1)
data1.reset_index(inplace=True)
data2.reset_index(inplace=True)# Reset Index
data_dict = data1.to_dict("records")  # Convert to dictionary
data_dict2 = data2.to_dict("records")
print(data_dict)
col.insert_one({"index": symbol, "data": data_dict})  # inesrt into DB
col.insert_one({"index": symbol, "data": data_dict2})
print('*')
#dfall = pd.DataFrame(index=[], columns=['Date', 'Open', "High", 'Low', 'Close', 'Adj Close', 'Symbol'])
#opendata = yf.Ticker(symbol).info.get('Open')
#df = dfall.append(opendata)
# stock_list = [{symbol}]
# df_list = pd.DataFrame()
# for stock in stock_list:
#     df = yf.download(stock, start_time)
#     df['stock'] = stock
#     df_list = pd.concat([df_list, df], axis=0)
#df_list.head()
#temp = datetime.timedelta('start_time', 'end_time')
#df = pd.DataFrame({"tickerdata"})
#df.reset_index(level=0, inplace=True)
#col.insert_many(df.to_dict('records'))
#df.to_dict('records')
# df
#temp = pd.DataFrame.between_time('start_time', 'end_time')
#print(temp)


########################## Filtering for last date in sort results: ##########################

#firstquary = db.data.findOne({'EndDate'})
for x in db.data.find():
    if db.data.count_documents({"symbol": "TSLA"})==0:
        ## creation of object of **new** symbol in our mongodb:
        ## --here we create--
        col.insert_one({"index": symbol, "data": data_dict})
        print("DataFrame.empty")

else:
    ## creation of additional object of **excisting** symbol in our mongodb - updating with upset:
    print('### what we store in the db:')
    col.insert_one({"index": symbol, "data": data_dict2})

    ## print of output
    print('### what we get straight from yf according to relevant dates:')
    print(ticker.history(start=retrievedate, end=end_time))


# dictionary --> date
# title --> key and then value
def write_signals_to_mongo(df, selectors, col, upsert=False):
    """
        Writes a DataFrame into a MongoDB collection
        if upsert then it will update existing records else mongo will append new data into the collection
    """
    #The iterrows() method generates an iterator object of the DataFrame, allowing us to iterate each row
    #in the DataFrame. Each iteration produces an index object and a row object (a Pandas Series object).

    if (upsert == True):
        for _, row in df.iterrows():
            selector = {
                selectors[0]: row[selectors[0]],
                selectors[1]: row[selectors[1]],
            }
            update = {"$set":  row.to_dict()}

            col.update_one(selector, update, upsert=True)
    else:
        col.insert_many(df.to_dict('records'))


        ### creation of unit tests for the following scenarios:
        # 1) inserting a large amount of data at once
        # 2) trying to update a key that does not exist
        # 3) searching for a type-o symbol
        # 4) searching without inserting a symbol
        # 5) inserting the wrong type of data as search input
        # 6) searching for more than one symbol
        # 7)