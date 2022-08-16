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

HashjoinExecutor(session, keyspace_name) \ 
    .join(tableinfoL, tableinfoR) \
    .execute() \
    .save_result("b_dummy_1_result") \
    .get_time_elapsed()

printJoinResult("b_dummy_1_result")