import psutil
from pympler import asizeof

from commands import *
from utils import *
from math_utils import *

from abc import ABC, abstractmethod
from cassandra.query import dict_factory


class JoinExecutor(ABC):
    def __init__(self, session, keyspace_name):
        # These attributes are about the DB from Cassandra
        self.session = session
        self.session.row_factory = dict_factory
        self.keyspace = keyspace_name

        # Saving commands for Lazy execution
        self.command_queue = []

        # Set a left table as the join process is a deep left-join
        self.left_table = "EMPTY"

        # Saving queries for each table needs (Select and Where Query)
        self.table_query = {}

        # Set join order to 1. Add 1 for every additional join command
        self.join_order = 1
        self.total_join_order = 1

        # Saving current result for the join process
        self.current_result = None
        self.current_result_column_names = None

        # Set the maximum size of data (Byte) that can be placed into memory simultaneously
        # Currently is set to 80% of available memory
        self.max_data_size = int(0.8 * psutil.virtual_memory().available)

        # Save all join information, this info will be used to execute join
        self.joins_info = []

        # Save all metadata about join tables
        self.join_metadata = JoinMetadata()

        # To force use partition method
        self.force_partition = False

        # To force save partition trace
        self.save_partition_trace = True


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


    def select(self, table, column, condition):
        # Inherited method

        command = SelectCommand(table, column, condition)
        self.command_queue.insert(0, command)

        return self

    @abstractmethod
    def execute(self):
        pass


class JoinMetadata:
    def __init__(self):
        super().__init__()
        self.tables = set()

        # Columns would be dict, key is table name and values are column names
        self.columns = {}

    def add_table(self, table_name):
        if (self.is_table_exists(table_name)):
            return

        self.tables.add(table_name)

        if (not table_name in self.columns):
            # Columns of table saved in LIST
            self.columns[table_name] = []

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

    def is_column_exists(self, table_name, column_name):
        if (not table_name in self.columns):
            return False
        
        if (not column_name in self.columns[table_name]):
            return False
        
        return True
    
    def get_columns_of_table(self, table_name):
        return self.columns[table_name]

    
    def get_size(self):

        return asizeof.asizeof(self)