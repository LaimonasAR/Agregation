""" 
#-----------Description--------------

Online store "Target" has a database containing their sales data, containing:
Product names;
Quantities sold to customer;
Price;
Customer;
date of purchase;

Task is to analyze given data to:

1. List all products sold in April.
2. Find the customer with most purchasses.
3. Gather all porducts purchased by that customer.
4. Find the highest selling product.

#-----------Tasks---------------------

1. Create database for store "Target" and collection "sales"
2. Populate "sales" collection with data.
3. Write aggregation pipelines for:
    3.1 Listing all products sold in April.
    3.2 Finding customer with most purchases.
    3.3 Gathering all products purchased by that customer
    3.4 Finding the highest selling product.

"""

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from typing import List, Dict, Any


class DbTarget:
    def __init__(self, host: str, port: int, db_name: str, collection: str) -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection = collection

    def connect_to_mongodb(self) -> Database:
        client = MongoClient(self.host, self.port)
        database = client[self.db_name]
        return database


# -----------April Sales----------------------------
class FilterAllProd:
    def __init__(self, collection: Collection, filter_criteria: List[Dict[str, Any]]):
        self.collection = collection
        self.filter_criteria = filter_criteria

    def filter_documents(self) -> Cursor:
        pipeline = [{"$match": {"$and": self.filter_criteria}}]
        return collection.aggregate(pipeline)


db_task = DbTarget(host="0.0.0.0", port=27017, db_name="Target", collection="sales")
db = db_task.connect_to_mongodb()
collection = db["sales"]


def find_april_sales():
    criteria: List[Dict[str, Any]] = [
        {
            "$and": [
                {"date": {"$gt": "2023-04-01T00:00:00Z"}},
                {"date": {"$lt": "2023-04-30T23:59:00Z"}},
            ]
        }
    ]

    april_sales = FilterAllProd(collection=collection, filter_criteria=criteria)
    for sale in april_sales.filter_documents():
        print(sale)


# -------------Best Customer--------------------------


class Agregation:
    def __init__(self, collection: Collection, pipeline: List[Dict[str, Any]]):
        self.collection = collection
        self.pipeline = pipeline

    def group_documents(self) -> Cursor:
        # pipeline = [{"$group": self.group_fields}]
        return self.collection.aggregate(self.pipeline)


def find_best_customer():
    pipeline = [
        {
            "$group": {
                "_id": {
                    "customer": "$customer",
                },
                "count": {"$sum": 1},
                "total_purchases": {"$sum": {"$multiply": ["$quantity", "$price"]}},
            },
        },
        {"$sort": {"total_purchases": -1}},
        {"$limit": 1},
    ]

    grouped = Agregation(collection, pipeline)  # Type: Cursor
    result = grouped.group_documents()

    for customer in result:
        print(customer)



if __name__ == "__main__":
    print("-----April sales begin----")
    find_april_sales()
    print("-----April sales end----")
    print("-----Best customer begin----")
    find_best_customer()
    print("-----Best customer end----")
