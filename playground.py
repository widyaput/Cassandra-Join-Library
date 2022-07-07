import time
from tabulate import tabulate
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.metadata import TableMetadata, ColumnMetadata


import os
import shutil

# User import
from utils import *



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

put_into_partition(dummy_data_per_iteration, 1, "ID")
put_into_partition(dummy_data_per_iteration, 2, "ID")
put_into_partition(dummy_data_per_iteration, 3, "ID")
put_into_partition(dummy_data_per_iteration, 4, "ID")
put_into_partition(dummy_data_per_iteration, 5, "ID")
put_into_partition(dummy_data_per_iteration, 6, "ID")

t2 = time.time()
print("Execution Time : ", t2-t1)