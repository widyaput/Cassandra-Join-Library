import time
from tabulate import tabulate
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.metadata import TableMetadata, ColumnMetadata

import psutil

# User import
from utils import *
from intermediate_result import IntermediateResult

cluster = Cluster()

# Keyspace, Table and Join Column Information
keyspace_name = 'ecommerce'
left_table = "user"
right_table = "payment_received"
join_column = "email"

device_status = dict(psutil.virtual_memory()._asdict())

print(device_status)
print(psutil.virtual_memory().percent)

