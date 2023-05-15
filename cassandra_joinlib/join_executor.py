import psutil
from pympler import asizeof
import json

from cassandra_joinlib.commands import *
from cassandra_joinlib.utils import *
from cassandra_joinlib.math_utils import *

from abc import ABC, abstractmethod
from cassandra.query import dict_factory
from cassandra.cluster import Session
from typing import Optional
from datetime import datetime
from base64 import b64encode, b64decode
from uuid import uuid4


class JoinExecutor(ABC):
    queue_name = "divide_jobs"
    exchange_name = "executor_exchange"
    
    def __init__(self, session: Session, keyspace_name: str, amqp_url = '', workers_count = -1):
        # These attributes are about the DB from Cassandra
        self.session = session
        if self.session:
            self.session.row_factory = dict_factory
        self.keyspace = keyspace_name

        self.amqp_url = amqp_url

        self.nodes_order = -1

        self.workers_count = workers_count

        self.leftest_table_size = -1

        # Saving commands for Lazy execution
        self.command_queue = []

        # Select command queue
        self.selected_cols = {}

        # Set a left table as the join process is a deep left-join
        self.left_table = "EMPTY"

        # Saving queries for each table needs (Select and Where Query)
        self.table_query = {}

        self.count_table_query = {}

        # Set join order to 1. Add 1 for every additional join command
        self.join_order = 1
        self.total_join_order = 1

        # Saving current result for the join process
        self.current_result = None
        self.current_result_column_names = None

        # Set the maximum size of data (Byte) that can be placed into memory simultaneously
        # Currently is set to 80% of available memory
        self.max_data_size = int(0.25 * psutil.virtual_memory().available)

        self.left_data_size = 0
        self.right_data_size = 0

        # Save all join information, this info will be used to execute join
        self.joins_info = []

        # Save all metadata about join tables
        self.join_metadata = JoinMetadata()

        # To force use partition method
        self.force_partition = False

        # To force save partition trace
        self.save_partition_trace = True

        # Cassandra details
        self.cassandra_fetch_size = 10000
        self.paging_state = {}

        # Save durations for each operation
        self.time_elapsed = {}

    def get_data_size(self):
        return self.left_data_size + self.right_data_size


    def join(self, leftTableInfo, rightTableInfo, join_operator = "="):
        # Append last

        join_type = "INNER"
        command = JoinCommand(join_type, leftTableInfo, rightTableInfo, join_operator)
        self.command_queue.append(command)

        return self

    def leftJoin(self, leftTableInfo, rightTableInfo, join_operator = "="):
        # Inherited method

        join_type = "LEFT_OUTER"
        command = JoinCommand(join_type, leftTableInfo, rightTableInfo, join_operator)
        self.command_queue.append(command)

        return self


    def rightJoin(self, leftTableInfo, rightTableInfo, join_operator = "="):
        # Inherited method

        join_type = "RIGHT_OUTER"
        command = JoinCommand(join_type, leftTableInfo, rightTableInfo, join_operator)
        self.command_queue.append(command)

        return self

    def fullOuterJoin(self, leftTableInfo, rightTableInfo, join_operator = "="):
        # Inherited method

        join_type = "FULL_OUTER"
        command = JoinCommand(join_type, leftTableInfo, rightTableInfo, join_operator)
        self.command_queue.append(command)

        return self


    def select(self, table, columns):
        # Inherited method

        columns = set(columns)
        command = SelectCommand(table, columns)
        self.command_queue.insert(0, command)

        return self

    # def where(self, expressions: Union[FilterExpression, FilterOperators]):
    #     command = FilterCommand(expressions)
    #     self.command_queue.append(command)
    #
    #     return self

    def filter_by(self, conditions: Condition):
        command = FilterCommands(conditions)
        self.command_queue.append(command)

        return self
        

    def selects_validation(self):
        # Can only do select when all join columns are selected

        if (self.selected_cols == {}):
            return True
        
        for command in self.command_queue:
            if (command.type != "JOIN"):
                continue
            
            left_alias = command.left_alias
            left_table = command.left_table
            if (left_alias != None):
                left_table = left_alias
            left_join_col = command.join_column

            is_left_table_exists = False

            try:
                is_left_table_exists = left_table in self.selected_cols

            except:
                pass

            if (is_left_table_exists):
                if (not left_join_col in self.selected_cols[left_table]):
                    print(f"Join column {left_join_col} in {left_table} are not selected!")
                    return False


            right_alias = command.right_alias            
            right_table = command.right_table
            if (right_alias != None):
                right_table = right_alias
            right_join_col = command.join_column_right

            is_right_table_exists = False
            try:
                is_right_table_exists = right_table in self.selected_cols

            except:
                pass
            
            if (is_right_table_exists):
                if (not right_join_col in self.selected_cols[right_table]):
                    print(f"Join column {right_join_col} in {right_table} are not selected!")
                    return False

        return True

    def get_time_elapsed(self):
        if (self.time_elapsed == {}):
            print("Join has not been executed!")
            return
        
        print("Details of time elapsed\n\n")
        join_time = self.time_elapsed['join']
        fetch_time = self.time_elapsed['data_fetch']
        total_time = self.time_elapsed['total']
        join_without_fetch = join_time - fetch_time

        print(f"Fetch Time: {fetch_time} s")
        print(f"Join without fetch time: {join_without_fetch} s")
        print(f"Join total time: {join_time} s")

        return self

    def execute(self, save_as=f"{datetime.now()}"):
        # for loop hingga sebanyak workernodes
        # lalu assign order_nodes dengan index
        # kirim ke mq
        assert(self.session is not None)
        
        if self.nodes_order == -1 and self.amqp_url:
            import pika
            import pickle

            connection = pika.BlockingConnection(
                pika.URLParameters(self.amqp_url)
                )
            channel = connection.channel()
            channel.exchange_declare(JoinExecutor.exchange_name, "direct")
            channel.queue_declare(queue=JoinExecutor.queue_name)
            channel.queue_bind(
                queue=JoinExecutor.queue_name,
                exchange=JoinExecutor.exchange_name,
                routing_key=JoinExecutor.queue_name
            )
            
            for command in self.command_queue:
                if isinstance(command, SelectCommand):
                    table = command.table
                    columns = command.columns

                    if (not table in self.selected_cols):
                        self.selected_cols[table] = columns

                    else :
                        self.selected_cols[table] = columns.union(self.selected_cols[table])    
                if isinstance(command, JoinCommand):
                    if (not self.selects_validation()):
                        print("Some join columns are not selected")
                        return
                    query = f"select count(*) as count from {self.keyspace}.{command.left_table}" 
                    result = self.session.execute(query)
                    self.leftest_table_size = result.one()['count']
                    break
            self.session = None
            from copy import deepcopy
            message_id = str(uuid4())
            for i in range(self.workers_count):
                executor = deepcopy(self)
                executor.nodes_order = i
                message = {
                    "executor":b64encode(pickle.dumps(executor)).decode('utf-8'),
                    "save_as":save_as,
                    "message_id": message_id,
                }
                channel.basic_publish(
                    exchange=JoinExecutor.exchange_name, 
                    routing_key=JoinExecutor.queue_name, 
                    body = json.dumps(message),
                )
            
            temp_queue = channel.queue_declare(queue="", exclusive=True)
            temp_queue_name = temp_queue.method.queue
            channel.queue_bind(
                exchange=JoinExecutor.exchange_name,
                queue=temp_queue_name,
                routing_key=message_id,
            )

            count = 0
            def cb(ch, method, properties, body):
                nonlocal count
                count += 1
                cwd = os.getcwd()
                folder = os.path.join(cwd, "results")
                if (not os.path.isdir(folder)):
                    os.mkdir(folder)
                file_path = os.path.join(folder, save_as + ".txt")
                with open(file_path, mode='a+') as file:
                    file.write(body.decode())

                if count == self.workers_count:
                    ch.stop_consuming()
                
            channel.basic_consume(queue=temp_queue_name, auto_ack=True, on_message_callback=cb)
            channel.start_consuming()
            
            connection.close()
        # jika ga ada mq yaudah

    @staticmethod
    def consume(amqp_url: str, session: Session):
        import pika
        import pickle

        connection = pika.BlockingConnection(
            pika.URLParameters(amqp_url)
            )
        channel = connection.channel()
        channel.exchange_declare(JoinExecutor.exchange_name, "direct")
        channel.queue_declare(queue=JoinExecutor.queue_name)
        channel.queue_bind(
            queue=JoinExecutor.queue_name,
            exchange=JoinExecutor.exchange_name,
            routing_key=JoinExecutor.queue_name
        )
        def cb(cn, method, properties, body):
            message_body = json.loads(body)
            executor: JoinExecutor = pickle.loads(b64decode(message_body["executor"].encode()))
            session.row_factory = dict_factory
            executor.session = session
            executor.execute(save_as=message_body["save_as"])
            cwd = os.getcwd()
            folder = os.path.join(cwd, "results")
            file_path = os.path.join(folder, message_body["save_as"] + "_" + str(executor.nodes_order) + ".txt")
            with open(file_path, 'r') as file:
                file_content = file.read()
            cn.basic_publish(
                exchange=JoinExecutor.exchange_name,
                routing_key=message_body["message_id"],
                body=file_content,
            )
        channel.basic_consume(
            queue=JoinExecutor.queue_name,
            on_message_callback=cb,
            auto_ack=True
        )
        channel.start_consuming()



class JoinMetadata:
    def __init__(self):
        super().__init__()
        self.tables = set()

        # Columns would be dict, key is table name and values are column names
        self.columns = {}

        self.pk_columns = {}

        self.clustering_columns = {}

    def add_table(self, table_name):
        if (self.is_table_exists(table_name)):
            return

        self.tables.add(table_name)

        if (not table_name in self.columns):
            # Columns of table saved in LIST
            self.columns[table_name] = []

        if (not table_name in self.pk_columns):
            self.pk_columns[table_name] = []

        if (not table_name in self.clustering_columns):
            self.clustering_columns[table_name] = []

    def is_table_exists(self, table_name):
        if (table_name in self.tables):
            return True

        return False

    def add_many_columns(self, table_name, columns):
        for col in columns:
            self.columns[table_name].append(col)
        
        return

    def add_one_column(self, table_name, column_name):
        self.columns[table_name].append(column_name)
        return
    
    def add_pk_column(self, table_name: str, column: str):
        self.pk_columns[table_name].append(column)

    def add_clustering_column(self, table_name: str, column: str):
        self.clustering_columns[table_name].append(column)

    def is_column_exists(self, table_name, column_name):
        return self.__check_column__(table_name, column_name)

    def is_pk_exists(self, table_name: str, column_name: str):
        return self.__check_column__(table_name, column_name, 'pk')

    def is_clusterkey_exists(self, table_name: str, column_name: str):
        return self.__check_column__(table_name, column_name, "cluster")

    def __check_column__(self, table_name: str, column_name: str, type: str = ''):
        columns = None
        if type == '':
            columns = self.columns
        if type == 'pk':
            columns = self.pk_columns
        if type == 'cluster':
            columns = self.clustering_columns
        assert columns is not None

        if (not table_name in columns):
            return False

        if (not column_name in columns[table_name]):
            return False
        
        return True
    
    def get_columns_of_table(self, table_name):
        return self.columns[table_name]

    def get_pk_columns_of_table(self, table_name):
        return self.pk_columns[table_name]

    
    def get_size(self):
        return asizeof.asizeof(self)
