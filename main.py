# Older version of Test.py

import time
import hashlib

from tabulate import tabulate
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.metadata import TableMetadata, ColumnMetadata

# User import
from utils import *


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

# [TODO] Checking Existence of join column

# Dictionary save key-value with
# Key is the join column
# Value is the IDs from left and right table


start_time = time.time()
# -------------------------- Start here -------------------------- 
column_names = get_column_names_from_db(session, keyspace_name, left_table)

partition_ids = {49}
column_names_2 = get_column_names_from_local(2, partition_ids)
print(column_names)
print(column_names_2)


# -------------------------- End here -------------------------- 
end_time = time.time()
print()
print(f"Execution Time : {end_time - start_time} s\n")