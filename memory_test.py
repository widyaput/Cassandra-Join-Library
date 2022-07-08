import os
import tempfile, shutil
import json

import psutil
from pympler import asizeof

from utils import *
from math_utils import *

from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement

def handle_success(rows):
    user = rows[0]
    try:
        print("A")
    except Exception:
        print("Failed to process user")

def handle_error(exception):
    print("Failed to fetch user info")


cluster = Cluster()

# Keyspace, Table and Join Column Information
keyspace_name = 'ecommerce'
left_table = "user"
right_table = "payment_received"
join_column = "email"

third_table = "user_item_like"
third_join_column = "userid"

session = cluster.connect(keyspace_name)

# Set the row factory as dictionary factory, ResultSet is List of Dictionary
session.row_factory = dict_factory

memory_usage = dict(psutil.virtual_memory()._asdict())
print(memory_usage)
print()

total_memory = byte_to_gigabyte(memory_usage['total'])
avail_memory = byte_to_gigabyte(memory_usage['available'])
used_memory = byte_to_gigabyte(memory_usage['used'])
free_memory = byte_to_gigabyte(memory_usage['free'])
wired_memory = byte_to_gigabyte(memory_usage['wired'])
active_memory = byte_to_gigabyte(memory_usage['active'])

memory_diff = total_memory - (avail_memory + used_memory + free_memory)
active_diff = used_memory - (wired_memory + active_memory)
used_percentage = ((used_memory) / total_memory) * 100

print("Total : ", total_memory, "GB")
print("Available : ", avail_memory, "GB")
print("Used : ", used_memory,"GB")
print("Free : ", free_memory, "GB")
print()
print("Used Percentage : ", used_percentage, "%")
print("Memory Difference : ", memory_diff, "GB")
print("Active Memory Difference : ", active_diff, "GB")