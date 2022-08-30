from cassandra.cluster import Cluster
from hash_join import *
from file_utils import *
from utils import *

cluster = Cluster()
keyspace_name = 'mubi_medium'
session = cluster.connect(keyspace_name)

table1 = "movie"
table2 = "rating"
join_column = "movie_id"

tableinfoL = TableInfo(table1, join_column)
tableinfoR = TableInfo(table2, join_column)

executor = HashJoinExecutor(session, keyspace_name) \
    .join(tableinfoL, tableinfoR) \
    .execute() \
    .save_result("b_mubimed_hash_result")

executor.get_time_elapsed()