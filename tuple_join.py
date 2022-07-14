import psutil
from pympler import asizeof

from commands import *
from utils import *
from math_utils import *

from cassandra.query import dict_factory
from join_executor import JoinExecutor

class TupleJoinExecutor(JoinExecutor):
    def __init__(self, session, keyspace_name, table_name):
        super().__init__(self, session, keyspace_name, table_name)
        
        # Left table partition ids
        self.left_table_last_partition_id = -1
        self.right_table_last_partition_id = -1
        self.result_max_partition_id = -1

        # Maximum size of partition of tuples
        self.partition_max_size = megabyte_to_byte(50)

        # Override force partition
        self.force_partition = False


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
            right_table = join_info['right_table']
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

            # Increment join_order num
            self.join_order += 1


        # Build final result here
        # TODO: Change code below for tuple join
        if (self.current_result == []): # Final result is in disk
            # For now, merge all results to memory
            # TODO: Merge to file or print per batch
            final_result = []
            partition_ids = self.current_join_partition_ids
            final_join_order = self.join_order

            for id in partition_ids:
                # Read data
                partition_data = read_from_partition(final_join_order, id, True)
                for row in partition_data:
                    # Convert to JSON then add to final result
                    row = json.loads(row[:-1])
                    final_result.append(row)

            # TODO: Delete all tmpfiles after join operation

            return final_result
        
        else : # Final result is in memory (self.current_result), return immediately

            return self.current_result


    def _get_left_data(self):
        # Only get data when fit to memory. Otherwise, load in join execution function
        session = self.session

        left_table_rows = None
        is_data_in_partitions = False
        left_last_partition_id = -1

        # Read data
        if (self.join_order == 1):
            left_table_query = self.table_query[self.left_table]

            # TODO: Use paging for bigger queries
            left_table_rows = session.execute(left_table_query)

        else : # Non first order join, left table may from partitions or cassandra
            if (self.current_result == []):
                print("Masuk disk")
                left_last_partition_id = self.left_table_last_partition_id
                is_data_in_partitions = True

                return left_table_rows, is_data_in_partitions, left_last_partition_id
                
            
            else :
                print("Masuk memory")
                left_table_rows = self.current_result


        # SIZE CHECKING
        if ((self.max_data_size <= asizeof.asizeof(left_table_rows)) or self.force_partition):
            is_data_in_partitions = True

            left_last_partition_id = put_into_partition_nonhash(left_table_rows, self.join_order, self.partition_max_size, self.left_table_last_partition_id, True)


        return left_table_rows, is_data_in_partitions, left_last_partition_id


    def _get_right_data(self, right_table):
        # Only get data when fit to memory. Otherwise, load in join execution function
        session = self.session

        right_table_rows = None
        is_data_in_partitions = False
        right_last_partition_id = -1

        right_table_query = self.table_query[right_table]

        # TODO: Use paging query
        right_table_rows = session.execute(right_table_query)

        # SIZE CHECKING
        if ((self.max_data_size <= asizeof.asizeof(right_table_rows)) or self.force_partition):
            is_data_in_partitions = True

            right_last_partition_id = put_into_partition_nonhash(right_table_rows, self.join_order, self.partition_max_size, self.right_table_last_partition_id, False)

        return right_table_rows, is_data_in_partitions, right_last_partition_id


    def _decide_join(self):
        pass


    def _execute_direct_join(self):
        pass


    def _execute_partition_join(self):
        pass

    
