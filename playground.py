import time
from pympler import asizeof
from tabulate import tabulate
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement
from cassandra.metadata import TableMetadata, ColumnMetadata


import os
import shutil

# User import
from utils import *
from file_utils import *
from join_executor import *

# Keyspace, Table and Join Column Information
keyspace_name = 'dummy'
table1 = "user"
table2 = "payment_received"
join_column = "email"

table3 = "user_item_like"
third_join_column = "userid"

cluster = Cluster()

session = cluster.connect(keyspace_name)

fetch_size = 10

class MyPlayground:
    def __init__(self):
        self.table_name = "Hello"

    def execute(self):
        query = "SELECT * FROM user"
        statement = SimpleStatement(query, fetch_size=100)
        total_rows = 0
        total_size = 0
        for row in session.execute(statement):
            total_rows += 1
            total_size += asizeof.asizeof(row)
            print(asizeof.asizeof(row))
            # print(self.get_size())
        
        print(total_rows)
        print(total_size)

    def get_size(self):
        return asizeof.asizeof(self)

# Set the row factory as dictionary factory, ResultSet is List of Dictionary
session.row_factory = dict_factory

meta = {}

# myObject = MyPlayground()
# myObject.execute()

myResults = [{
    "ID" : 1,
    "Name" : "Rafi",
    },
    {
        "ID" : 2,
        "Name" : "Raissa",
    },
    {
        "ID" : 3,
        "Name" : "Adnan",
    }
    ]

print_result_as_table(myResults)
print_result_as_table(myResults)

