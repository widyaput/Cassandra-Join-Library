from cassandra.cluster import Cluster
from nested_join import *
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

executor = NestedJoinExecutor(session, keyspace_name) \
    .fullOuterJoin(tableinfoL, tableinfoR) \
    .execute() \
    .save_result("b_dummy_5_result") \

printJoinResult("b_dummy_5_result")
executor.get_time_elapsed()