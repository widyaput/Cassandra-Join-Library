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
session.default_fetch_size = 1

# Set the row factory as dictionary factory, ResultSet is List of Dictionary
session.row_factory = dict_factory

query = "SELECT * FROM user"

user_ps = session.prepare("SELECT * FROM user")
user_ps.fetch_size = 1

statement = SimpleStatement(query, fetch_size=10)

rows = list(session.execute(statement))
print(len(rows))

