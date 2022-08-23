from cassandra.cluster import Cluster
from nested_join import *
from file_utils import *
from utils import *

cluster = Cluster()
keyspace_name = 'ecommerce'
session = cluster.connect(keyspace_name)

table1 = "user"
table2 = "user_item_like"
join_column = "userid"

tableinfoL = TableInfo(table1, join_column)
tableinfoR = TableInfo(table2, join_column)

NestedJoinExecutor(session, keyspace_name) \
    .rightJoin(tableinfoL, tableinfoR, ">") \
    .execute() \
    .save_result("a_nested_3_result")

printJoinResult("a_nested_3_result")

