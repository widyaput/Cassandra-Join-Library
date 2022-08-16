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

table3 = "rating"
second_join_column = "userid"

tableinfo1_L = TableInfo(table1, join_column)
tableinfo1_R = TableInfo(table2, join_column)

tableinfo2_L = TableInfo(table1, second_join_column)
tableinfo2_R = TableInfo(table3, second_join_column)

NestedjoinExecutor(session, keyspace_name) \ 
    .fullOuterJoin(tableinfo1_L, tableinfo1_R) \
    .join(tableinfo2_L, tableinfo2_R) \
    .execute() \
    .save_result("a_nested_5_result")

printJoinResult("a_nested_5_result")

