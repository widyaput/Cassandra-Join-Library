from cassandra.cluster import Cluster
from hash_join import *
from file_utils import *
from utils import *

cluster = Cluster()
keyspace_name = 'ecommerce'
session = cluster.connect(keyspace_name)

table1 = "user"
table2 = "payment_received"
join_column = "email"

tableinfoL = TableInfo(table1, join_column)
tableinfoR = TableInfo(table2, join_column)

HashJoinExecutor(session, keyspace_name) \
    .join(tableinfoL, tableinfoR) \
    .execute() \
    .save_result("a_hash_1_result")

printJoinResult("a_hash_1_result")