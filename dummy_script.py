import requests
import json

from cassandra.cluster import Cluster
from cassandra.query import dict_factory, SimpleStatement
from cassandra.metadata import TableMetadata, ColumnMetadata

api_link = "https://api.namefake.com/indonesian-indonesia/"

keyspace = "dummy"
table = "user"

cluster = Cluster()

session = cluster.connect(keyspace)


for id in range(9079, 10000):
    api_response = requests.get(api_link)
    data = api_response.text
    json_data = json.loads(data)

    name = json_data['name']
    email = json_data['email_u']
    print(f"{id} - {name} / {email}\n")

    query = f"INSERT INTO {keyspace}.{table} (userid, name, email) VALUES ({id}, '{name}', '{email}')"
    session.execute(query)
