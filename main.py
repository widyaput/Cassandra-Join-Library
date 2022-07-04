# Older version of Test.py

import time
import hashlib

from tabulate import tabulate
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.metadata import TableMetadata, ColumnMetadata

# User import
from utils import *
from intermediate_result import IntermediateResult


cluster = Cluster()

# Keyspace, Table and Join Column Information
keyspace_name = 'ecommerce'
left_table = "user"
right_table = "payment_received"
join_column = "email"


session = cluster.connect(keyspace_name)

# Set the row factory as dictionary factory, ResultSet is List of Dictionary
session.row_factory = dict_factory

# [TODO] Checking Existence of join column


intermediate_result = IntermediateResult(join_column)
# Dictionary save key-value with
# Key is the join column
# Value is the IDs from left and right table


start_time = time.time()
# -------------------------- Start here -------------------------- 


result = None

# Check whether join columns exists in left and right table
left_table_res = None
if (result == None):
    left_table_res = session.execute(f'SELECT * FROM {left_table} LIMIT 1').one()
    
else :
    # Intermediate Result is not None
    left_table_res = result[0]

right_table_res = session.execute(f'SELECT * FROM {right_table} LIMIT 1').one()

if (not (join_column in left_table_res)) :
    print("Throw Error, Join column does not exist in left table")

elif (not(join_column in right_table_res)):
    print("Throw Error, Join column is not exist in right table")


# Left table iteration
user_rows = session.execute(f'SELECT * FROM {left_table}')

for user_row in user_rows:
    intermediate_result.add_row_to_intermediate(user_row, True)



# Right table iteration
payment_rows = session.execute(f'SELECT * FROM {right_table}')

for payment_row in payment_rows:
    intermediate_result.add_row_to_intermediate(payment_row, False)



result = intermediate_result.build_result()
# print_result_as_table(result)
print(result)
# print(intermediate_result.hash_table)

# Printing Part
# table = tabulate(user_rows, headers=['userid', 'email', 'name'], tablefmt='orgtbl')

# print(table)


# -------------------------- End here -------------------------- 
end_time = time.time()
print()
print(f"Execution Time : {end_time - start_time} s\n")