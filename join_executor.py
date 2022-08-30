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

        # Select command queue
        self.selected_cols = {}

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

    @abstractmethod
    def execute(self):
        pass

    # @abstractmethod
    def save_result(self, filename):
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
