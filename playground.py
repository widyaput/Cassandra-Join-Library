import time
from pympler import asizeof
from tabulate import tabulate
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.metadata import TableMetadata, ColumnMetadata


import os
import shutil

# User import
from utils import *
from file_utils import *
from join_executor import *



number_of_iterations = 5
join_column = "ID"

dummy_data_per_iteration = [
    {
        "ID" : 1,
        "Name" : "Rafi Adyatma"
    },
    {
        "ID" : 2,
        "Name" : "Raissa Azzahra"
    },
    {
        "ID" : 3,
        "Name" : "Adnan Wick"
    },
    {
        "ID" : 4,
        "Name" : "Farid Adika"
    },
    {
        "ID" : 5,
        "Name" : "Rio Aditia"
    },
    {
        "ID" : 6,
        "Name" : "Putri Devi"
    },
    {
        "ID" : 7,
        "Name" : "Faskal Rama"
    },
    {
        "ID" : 1,
        "Name" : "(Shadow) Rafi Adyatma"
    }
]

t1 = time.time()

the_data = [
    [{
        ("ID", "user") : 4,
        ("Name", "user") : "Rafi Adyatma",
        ("Email", "user") : "rafi.adyatma@gmail.com",
        ("Name", "item") : "Long Sword"
    },0],
    [{
        ("ID", "user") : 4,
        ("Name", "user") : "Rafi Adyatma",
        ("Email", "user") : "rafi.adyatma@gmail.com",
        ("Name", "item") : "Long Sword"
    },0]
]

print("Original data : ", the_data, "\n")

json_accepted_data = jsonTupleKeyEncoder(the_data)
the_json = json.dumps(json_accepted_data)

read_json = json.loads(the_json)
print("Readed json : ", read_json, "\n")
revert_data = jsonTupleKeyDecoder(read_json)
print("Reverted data : ", revert_data, "\n")

t2 = time.time()
print("Execution Time : ", t2-t1)