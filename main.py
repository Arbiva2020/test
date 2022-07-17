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
cluster = 'mongodb+srv://arbiva:Adi101010@cluster0.86nym.mongodb.net/test?retryWrites=true&w=majority'
client = MongoClient(cluster)
print(client.list_database_names())

########################## Accessing a database:################################
db = client.test
col = db.data
# print(db.list_collection_names())
# print(db["data"])
# print(col)

######################## Count times of TSLA apperence in database: #############
isthere = col.count_documents({"symbol": "TSLA"})
# print(isthere)

########################## Print all documents: ##########################
allmydocs = col.find()
 # for x in allmydocs:
 #    print(x)
##print("********")

##################### Print the last item (newest) and than print all documents in descending order: #############
mydoc = col.find().sort("EndDate", -1)
mylast = col.find_one()
retrievedate = col.find_one()["EndDate"]
print(mylast)
print(retrievedate)
print("*****")
for x in mydoc:
   print(x)
print("********")

########################## Accessing the collection by name: ##########################
data = db.data

########################## deleting documents of the same attribute (in this case, index) from db: #################
query = {"index": "TSLA"}
d = col.delete_many(query)

########################## Working with yf and tickers: ################################
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

################## assigning symbol_id into the pdr (dataFrame) ###################
print("##")
df = data2.assign(project_id="symbol")
# another way to open a new column:
df["project_id"] = "symbol"
print(df)
print('##')

print('**')
# for x in data_dict2:
#     print(x)
print('**')

print('***')
# print(data_dict2)
print('***')
# print(type(data2))
# print(type(data_dict2))
dic = df.to_dict('records')
print(dic)
print(type(dic))
print(type(df))
########################## Filtering for last date in sort results: ##########################


for x in db.data.find():
    if db.data.count_documents({"symbol": "TSLA"}) == 0:
        col.insert_many(df.to_dict('records'))
        print("DataFrame.empty")

else:
    ## creation of additional object of **excisting** symbol in our mongodb - updating with upset:
    print('### what we store in the db:')
    #col.insert_one({"index": symbol, "data": data_dict2})
    #for x in df:
    col.insert_many(df.to_dict('records'))
        #col.insert_one(df.to_dict('records'))
    # dict=df.to_dict('dict')
    # col.insert_one(dict)
    # dict1 = df.set_index("Date").T.to_dict('list')
    # col.insert_one(dict1)
    ## print of output
    print('### what we get straight from yf according to relevant dates:')
    print(ticker.history(start=retrievedate, end=end_time))


        ########################## creation of unit tests for the following scenarios: ##########################
        # 1) inserting a large amount of data at once
        # 2) trying to update a key that does not exist
        # 3) searching for a type-o symbol
        # 4) searching without inserting a symbol
        # 5) inserting the wrong type of data as search input
        # 6) searching for more than one symbol
        # 7)