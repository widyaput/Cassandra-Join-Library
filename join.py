import psutil
from pympler import asizeof

from commands import *
from intermediate_result import IntermediateHashResult
from cassandra.query import dict_factory

class JoinExecutor:
    def __init__(self, session, keyspace_name, table_name):
        super().__init__()
        # These attributes are about the DB from Cassandra
        self.session = session
        self.session.row_factory = dict_factory

        self.keyspace = keyspace_name

        # Saving current result for the join process
        self.current_result = None

        # Saving commands for Lazy execution
        self.command_queue = []

        # Set a left table as the join process is a deep left-join
        self.left_table = table_name

        # Saving queries for each table needs (Select and Where Query)
        self.table_query = {}

        # Saving partition ID for current join
        self.current_join_partition_id = {}

        # Set default select query on left_table
        self.table_query[table_name] = f"SELECT * FROM {table_name}"

        # Set join order to 1. Add 1 for every additional join command
        self.join_order = 1
        self.total_join_order = 1

        # Set the maximum size of data (Byte) that can be placed into memory simultaneously
        # Currently is set to 80% of available memory
        self.max_data_size = int(0.8 * psutil.virtual_memory().available)

        # Save all join information, this info will be used to execute join
        self.joins_info = []


    def setLeftTable(self, table, column):
        self.left_table = table
        self.left_column = column


    def join(self, right_table, join_column, join_column_right = None):
        # Append last

        join_type = "INNER"
        command = JoinCommand(join_type, right_table, join_column, join_column_right)
        self.command_queue.append(command)

        return self

    def leftJoin(self, right_table, join_column, join_column_right = None):
        # Append last

        join_type = "LEFT"
        command = JoinCommand(join_type, right_table, join_column, join_column_right)
        self.command_queue.append(command)

        return self


    def rightJoin(self, right_table, join_column, join_column_right = None):
        # Append last

        join_type = "RIGHT"
        command = JoinCommand(join_type, right_table, join_column, join_column_right)
        self.command_queue.append(command)

        return self


    def select(self, table, column, condition):
        # Append first
        command = SelectCommand(table, column, condition)
        self.command_queue.insert(0, command)

        return self

    def execute(self):
        # Consume the commands queue, execute join

        session = self.session

        # Use CQLBuilder (or maybe not) and save in self.table_query

        for command in self.command_queue :
            command_type = command.type

            if (command_type == "SELECT"):
                select_command = command
                table = select_command.table
                column = select_command.column_name
                condition = select_command.condition

                if (table in self.table_query):
                    self.table_query[table] += f"AND {column} {condition} "
                
                else :
                    self.table_query[table] = f"WHERE {column} {condition} "

                return

            elif (command_type == "JOIN"):
                join_command = command
                
                # Define join variables
                join_type = join_command.join_type
                right_table = join_command.right_table
                join_column = join_command.join_column
                join_column_right = join_command.join_column_right

                if (join_column_right == None): #Join column name is same on both table
                    join_column_right = join_column

                # Checking whether if join column is exist
                table_cols = []

                check_cols_query = f"SELECT * FROM system_schema.columns where keyspace_name = '{self.keyspace}' AND table_name = '{right_table}'"

                meta_rows = session.execute(check_cols_query)
                
                for row in meta_rows:
                    table_cols.append(row['column_name'])
        
                if not(join_column_right in table_cols):
                    # Throw error
                    print("Join column is not in right-table")


                # Build select query
                select_query = "SELECT"

                for idx in range(len(table_cols)):
                    col = table_cols[idx]

                    # Column naming
                    if (col == join_column_right):
                        select_query += f" {join_column_right} AS {join_column}"
                    else :
                        select_query += f" {col}"
                    

                    # End of each column select
                    if (idx != len(table_cols) - 1):
                        select_query += ","
                    else :
                        select_query += f" FROM {right_table} "


                # Append to self.table_query
                # Add new query or update existing query for each table involved
                if (right_table in self.table_query):
                    self.table_query[right_table] = select_query + self.table_query[right_table]
                
                else :
                    self.table_query[right_table] = select_query

                curr_join_info = {
                    "join_order" : self.join_order,
                    "join_type" : join_type,
                    "right_table" : right_table,
                    "join_column" : join_column
                }

                self.total_join_order += 1

                # To be proceeded later
                self.joins_info.append(curr_join_info)


        # Execute all joins based on self.joins_info
        for join_info_idx in range(len(self.joins_info)):
            join_info = self.joins_info[join_info_idx]

            join_type = join_info['join_type']
            right_table = join_info['join']
            join_column = join_info['join_column']

            # Next join column will be used to partition the result of current join
            # So that they can be used directly for the next join
            next_join_column = None
            if (join_info_idx == (len(self.joins_info) - 1)):
                # Current join is the final join
                next_join_column = None

            else :
                next_join_column = self.joins_info[join_info_idx+1]['join_column']

            # Join preparation has been prepared, now continue to execute_join
            self._decide_join(join_type, right_table, join_column, next_join_column)
            # or execute partitioned-join

        return self.current_result


    def _get_required_data(self, join_type, right_table, join_column):

        session = self.session

        left_table_rows = None
        right_table_rows = None

        # Read data
        if (self.current_result == None): # Current Result is None
            
            # Left table is from self.left_table
            left_table_query = self.table_query[self.left_table]

            print("Left-Table query : ", left_table_query)

            # TODO: Use paging query
            left_table_rows = session.execute(left_table_query)

            # SIZE CHECKING
            if (self.max_data_size <= asizeof.asizeof(left_table_rows)): 
                # Left table is bigger than max data size in memory, use partition
                # TODO: Divide data into partitions
                return False


            # Right table
            right_table_query = self.table_query[right_table]

            print("Right-Table query : ", right_table_query)

            # TODO: Use paging query
            right_table_rows = session.execute(right_table_query)

            # SIZE CHECKING
            if (self.max_data_size <= asizeof.asizeof(left_table_rows) + asizeof.asizeof(right_table_rows)): 
                # Left table is bigger than max data size in memory, use partition
                # TODO: Divide data into partitions
                return False
        
        else : # Current Result is Available
            
            left_table_rows = None
            
            # Decide the source of left table
            if (self.current_result == []): 
                # All current result has been flushed, read from partition
                left_table_rows = read_from_partition(1,2)

            else:
                # Left table is from self.current_result
                left_table_rows = self.current_result

            # Right table
            right_table_query = self.table_query[right_table]

            print("Right-Table query : ", right_table_query)

            # TODO: Use paging query
            right_table_rows = session.execute(right_table_query)


        return left_table_rows, right_table_rows


    def _decide_join(self, join_type, right_table, join_column, next_join_column):
        

        return


    def _execute_partition_join(self, join_type, right_table, join_column, next_join_column):

        return
    
    
    def _execute_join(self, join_type, right_table, join_column, next_join_column):

        session = self.session

        # Check whether current result is available
        intermediate_result = IntermediateHashResult(join_column, self.join_order,self.max_data_size, next_join_column)
        
        left_table_rows, right_table_rows = self._get_required_data(join_type, right_table, join_column)
        
        # TODO : Add rows to intermediate_result based on join type (Inner/ Outer)
        # Also consider non equi-join

        # Left table insertion to intermediate_result and clear left table rows for each iteration
        for idx in range(len(left_table_rows)):
            row = left_table_rows[idx]
            intermediate_result.add_row_to_intermediate(row, True)
            left_table_rows[idx] = None

        # Right table insertion to intermediate_result and clear right table rows for each iteration
        for idx in range(len(right_table_rows)):
            row = right_table_rows[idx]
            intermediate_result.add_row_to_intermediate(row, False)
            right_table_rows[idx] = None

        # Clear both left_table_rows and right_table_rows
        left_table_rows = None
        right_table_rows = None
        
        result = intermediate_result.build_result()

        # Append result as current result
        self.current_result = result

        return


    def _should_use_tempfile(self):
        
        return 

    
    def _flush_current_res_to_memory(self):

        return