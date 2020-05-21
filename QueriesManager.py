from pymongo import MongoClient
from pprint import pprint
import time
import sys

db = MongoClient('mongodb://mongo1:9042,mongo2:9142,mongo3:9242/?replicaSet=mongo-set').dataDB
data = db.datacollection

start1 = time.time()
documents = data.aggregate([{"$unwind": "$productStocks"}, 
{"$match": {"productStocks.quantity": 1}}, 
{"$project": {"_id": 0, "city": 0, "streetAddress": 0, "zipCode": 0}}
])
end1 = time.time()

f = open("test1.out", "w")
sys.stdout = f
for doc in documents:
    pprint(doc)

start2 = time.time()
documents = data.aggregate([{"$unwind": "$productStocks"}, 
{"$match": { "$or": [{"productStocks.product.madeFrom.id": 1}, {"productStocks.product.madeFrom.id": 2}, {"productStocks.product.madeFrom.id": 3}], 
"productStocks.quantity": {"$gt": 0}, "productStocks.product.category.id": 1}}, 
{"$project": {"_id": 0, "city": 0, "id": 0, "streetAddress": 0, "zipCode": 0}},
])
end2 = time.time()

f = open("test2.out", "w")
sys.stdout = f
for doc in documents:
   pprint(doc)

start3 = time.time()
documents = data.aggregate([{"$unwind": "$productStocks"}, 
{"$match": {"productStocks.quantity": {"$gt": 0}, "productStocks.product.category.id": 1, "id": 1}}, 
{"$project": {"_id": 0, "city": 0, "streetAddress": 0, "zipCode": 0}},
{"$sort": {"productStocks.product.price": 1}},
{"$limit": 10}
])
end3 = time.time()

f = open("test3.out", "w")
sys.stdout = f
for doc in documents:
    pprint(doc)


f = open("times.out", "w")
sys.stdout = f
print("First: " + format(float(end1 - start1), '.7f') + " sec")
print("Second: " + format(float(end2 - start2), '.7f') + " sec")
print("Third: " + format(float(end3 - start3), '.7f') + " sec")
f.close()

