import logging
from cassandra.connection import Event
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement

class PagedResultHandler(object):

    def __init__(self, future):
        self.error = None
        self.finished_event = Event()
        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_err)

    def handle_page(self, rows):
        for row in rows:
            # process_row(row)
            print(row)

        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        self.error = exc
        self.finished_event.set()


# Test handler below
cluster = Cluster([], )

# Keyspace, Table and Join Column Information
keyspace_name = 'ecommerce'

session = cluster.connect(keyspace_name)

# Set the row factory as dictionary factory, ResultSet is List of Dictionary
session.row_factory = dict_factory

query = "SELECT * FROM user"

statement = SimpleStatement(query, fetch_size=1)
results = session.execute(statement)

for row in results:
    print(row)

# save state somewhere
saved_ps = results.paging_state
print(results)
print(results.paging_state)

# resume pagination sometime later
ps = saved_ps
results = session.execute(statement, paging_state=ps)

print()


def handle_success(rows):
    for row in rows:
        print(row, flush=True)

def handle_error(exception):
    print("Failed to fetch users")

resultset = []

future = session.execute_async(query)
future.add_callbacks(callback=handle_success, errback=handle_error) 