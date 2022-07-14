import psutil
from pympler import asizeof

from commands import *
from utils import *
from math_utils import *

from abc import ABC, abstractmethod
from cassandra.query import dict_factory


class JoinExecutor(ABC):
    def __init__(self, session, keyspace_name, table_name):
        # These attributes are about the DB from Cassandra
        self.session = session
        self.session.row_factory = dict_factory
        self.keyspace = keyspace_name

        # Saving commands for Lazy execution
        self.command_queue = []

        # Set a left table as the join process is a deep left-join
        self.left_table = table_name

        # Saving queries for each table needs (Select and Where Query)
        self.table_query = {}

        # Set default select query on left_table
        self.table_query[table_name] = f"SELECT * FROM {table_name}"

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

        # To force use partition method
        self.force_partition = False


    def join(self, right_table, join_column, join_column_right = None):
        # Append last

        join_type = "INNER"
        command = JoinCommand(join_type, right_table, join_column, join_column_right)
        self.command_queue.append(command)

        return self

    def leftJoin(self, right_table, join_column, join_column_right = None):
        # Inherited method

        join_type = "LEFT_OUTER"
        command = JoinCommand(join_type, right_table, join_column, join_column_right)
        self.command_queue.append(command)

        return self


    def rightJoin(self, right_table, join_column, join_column_right = None):
        # Inherited method

        join_type = "RIGHT_OUTER"
        command = JoinCommand(join_type, right_table, join_column, join_column_right)
        self.command_queue.append(command)

        return self

    def fullOuterJoin(self,right_table, join_column, join_column_right = None):
        # Inherited method

        join_type = "FULL_OUTER"
        command = JoinCommand(join_type, right_table, join_column, join_column_right)
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