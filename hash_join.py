import psutil
from pympler import asizeof

from commands import *
from intermediate_result import IntermediateDirectHashResult, IntermediatePartitionedHashResult
from utils import *
from math_utils import *

from cassandra.query import dict_factory
from join_executor import JoinExecutor

class HashJoinExecutor(JoinExecutor):
    def __init__(self, session, keyspace_name):
        super().__init__(session, keyspace_name)
        
        # Saving partition ID for current join
        self.current_join_partition_ids = set()

        # Override force partition
        self.force_partition = True

        # To force save partition trace
        self.save_partition_trace = True


    def execute(self):
        # Inherited abstract method
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
                real_left_table = join_command.left_table
                real_right_table = join_command.right_table
                join_column = join_command.join_column
                join_column_right = join_command.join_column_right

                left_alias = join_command.left_alias
                right_alias = join_command.right_alias

                if (join_column_right == None): #Join column name is same on both table
                    join_column_right = join_column

                # Checking whether if join column is exist
                table_cols = []

                left_table = real_left_table
                if (left_alias != None):
                    left_table = left_alias

                right_table = real_right_table
                if (right_alias != None):
                    right_table = right_alias

                
                is_lefttable_in_metadata = self.join_metadata.is_table_exists(left_table)
                is_righttable_in_metadata = self.join_metadata.is_table_exists(right_table)



                check_leftcols_query = f"SELECT * FROM system_schema.columns where keyspace_name = '{self.keyspace}' AND table_name = '{real_left_table}'"
                check_rightcols_query = f"SELECT * FROM system_schema.columns where keyspace_name = '{self.keyspace}' AND table_name = '{real_right_table}'"

                if (not is_lefttable_in_metadata):
                    left_meta_rows = session.execute(check_leftcols_query)
                    self.join_metadata.add_table(left_table)

                    for row in left_meta_rows:
                        column_name = row['column_name']
                        self.join_metadata.add_one_column(left_table, column_name)
        
                if (not left_table in self.table_query):
                    self.table_query[left_table] = f"SELECT * FROM {real_left_table}"

                # Below are actions for Right table
                if (not is_righttable_in_metadata):
                    right_meta_rows = session.execute(check_rightcols_query)
                    self.join_metadata.add_table(right_table)

                    for row in right_meta_rows:
                        column_name = row['column_name']
                        self.join_metadata.add_one_column(right_table, column_name)

                
                table_cols = self.join_metadata.get_columns_of_table(right_table)
                if not(join_column_right in table_cols):
                    # Throw error
                    print("Join column is not in right-table")


                # Build select query
                select_query = "SELECT"

                for idx in range(len(table_cols)):
                    col = table_cols[idx]

                    # Column naming
                    # if (col == join_column_right):
                    #     select_query += f" {join_column_right} AS {join_column}"
                    # else :
                    #     select_query += f" {col}"
                    
                    select_query += f" {col}"

                    # End of each column select
                    if (idx != len(table_cols) - 1):
                        select_query += ","
                    else :
                        select_query += f" FROM {real_right_table} "


                # Append to self.table_query
                # Add new query or update existing query for each table involved
                if (right_table in self.table_query):
                    self.table_query[right_table] = select_query + self.table_query[right_table]
                
                else :
                    self.table_query[right_table] = select_query


                curr_join_info = {
                    "join_order" : self.total_join_order,
                    "join_type" : join_type,
                    "left_table" : real_left_table,
                    "right_table" : real_right_table,
                    "join_column" : join_column,
                    "join_column_right" : join_column_right,
                    "left_alias" : left_alias,
                    "right_alias" : right_alias
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
            next_join_table = None
            next_join_column = None
            if (join_info_idx == (len(self.joins_info) - 1)):
                # Current join is the final join
                next_join_table_alias = self.joins_info[join_info_idx]['left_alias']
                
                next_join_table = None
                if (next_join_table_alias == None):
                    next_join_table = self.joins_info[join_info_idx]['left_table']
                else :
                    next_join_table = next_join_table_alias

                next_join_column = self.joins_info[join_info_idx]['join_column']

            else :
                next_join_table_alias = self.joins_info[join_info_idx+1]['left_alias']

                if (next_join_table_alias == None):
                    next_join_table = self.joins_info[join_info_idx]['left_table']
                else :
                    next_join_table = next_join_table_alias

                next_join_column = self.joins_info[join_info_idx+1]['join_column']

            next_join_info = (next_join_column, next_join_table)

            # Join preparation has been prepared, now continue to execute_join
            self._decide_join(join_info, next_join_info)
            # or execute partitioned-join

            # Increment join_order num
            self.join_order += 1


        # Build final result here

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
                    final_result.append(row)

            # TODO: Delete all tmpfiles after join operation

            return final_result
        
        else : # Final result is in memory (self.current_result), return immediately

            return self.current_result

    def _get_left_data(self, left_table, join_column, left_alias):
        session = self.session

        left_table_rows = None
        is_data_in_partitions = False
        partition_ids = set()

        left_table_name = left_table
        if (left_alias != None):
            left_table_name = left_alias

        print(self.table_query)

        # Read data
        if (self.join_order == 1): # First join, all data are in Cassandra
            left_table_query = self.table_query[left_table_name]
            print("Left-Table query : ", left_table_query)

            # TODO: Use paging and async
            left_table_rows = session.execute(left_table_query)
            left_table_rows = list(left_table_rows)

            # Change the row structure
            for idx in range(len(left_table_rows)):
                left_row = left_table_rows[idx]

                tupled_key_dict = {}
                for key in left_row:
                    value = left_row[key]
                    new_key = (key, left_table_name)
                    tupled_key_dict[new_key] = value
                
                left_table_rows[idx] = tupled_key_dict



        else : # Non first join, left table may in self.current_result or in partitions
            if (self.current_result == []): # Result of previous join is in local disk as partitions
                print("Masuk disk")
                left_table_rows = None
                is_data_in_partitions = True
                partition_ids = self.current_join_partition_ids

                print("Left table partition ids : ", self.current_join_partition_ids)

                return left_table_rows, is_data_in_partitions, partition_ids

            else :
                print("Masuk memory")
                print(self.current_result)
                left_table_rows = self.current_result
                
                # Reset current result to preserve memory
                self.current_result = []

        # SIZE CHECKING
        if ((self.max_data_size <= asizeof.asizeof(left_table_rows)) or self.force_partition): 
            # Left table is bigger than max data size in memory, use partition
            is_data_in_partitions = True

            # Flush left table
            partition_ids = put_into_partition(left_table_rows, self.join_order, left_table_name, join_column, True)
            left_table_rows = None

        return left_table_rows, is_data_in_partitions, partition_ids


    def _get_right_data(self, right_table, join_column, right_alias, is_left_table_partitioned):

        session = self.session

        right_table_rows = None
        is_data_in_partitions = False
        partition_ids = set()

        right_table_name = right_table

        if (right_alias != None):
            right_table_name = right_alias

        right_table_query = self.table_query[right_table_name]
        print("Right-Table query : ", right_table_query)

        # TODO: Use paging and async
        right_table_rows = session.execute(right_table_query)
        right_table_rows = list(right_table_rows)

        # Change the row structure to a tupled-key dict
        for idx in range(len(right_table_rows)):
            right_row = right_table_rows[idx]

            tupled_key_dict = {}
            for key in right_row:
                value = right_row[key]
                new_key = (key, right_table_name)
                tupled_key_dict[new_key] = value
            
            right_table_rows[idx] = tupled_key_dict


        # SIZE CHECKING
        if (((self.max_data_size <= asizeof.asizeof(right_table_rows)) or (is_left_table_partitioned)) or self.force_partition): 
            # Left table is bigger than max data size in memory, use partition
            is_data_in_partitions = True
            
            # Flush right table
            partition_ids = put_into_partition(right_table_rows, self.join_order, right_table_name, join_column, False)
            right_table_rows = None

        return right_table_rows, is_data_in_partitions, partition_ids


    def _decide_join(self, join_info, next_join_info):
        # Try to load data first in here, then decide how the join will be implemented
        # Directly or Partitioned
        next_join_column = next_join_info[0]
        next_join_table = next_join_info[1]

        left_table = join_info['left_table']
        right_table = join_info['right_table']

        join_column = join_info['join_column']
        join_column_right = join_info['join_column_right']

        left_alias = join_info['left_alias']
        right_alias = join_info['right_alias']

        left_table_name = left_table
        if (left_alias != None):
            left_table_name = left_alias
        
        right_table_name = right_table
        if (right_alias != None):
            right_table_name = right_alias

        # Get Left table data
        left_table_rows, is_left_table_in_partitions, left_partition_ids = self._get_left_data(left_table, join_column, left_alias)

        # Get right table data
        right_table_rows, is_right_table_in_partitions, right_partition_ids = self._get_right_data(right_table, join_column_right, right_alias, is_left_table_in_partitions)

        

        # Check size
        if ((not is_left_table_in_partitions) and (not is_right_table_in_partitions)):
            if (self.max_data_size <= (asizeof.asizeof(left_table_rows) + asizeof.asizeof(right_table_rows))):
                # Flush both tables
                left_partition_ids = put_into_partition(left_table_rows, self.join_order, left_table_name, join_column, True)
                right_partition_ids = put_into_partition(right_table_rows, self.join_order, right_table_name, join_column, False)

                is_left_table_in_partitions = True
                is_right_table_in_partitions = True


        # Do join based on whether partitions is used or not
        if (is_left_table_in_partitions and is_right_table_in_partitions):
            all_partition_ids = left_partition_ids.union(right_partition_ids)

            print("Left partition ids : ", left_partition_ids)
            print("Right partition ids : ", right_partition_ids)
            print("Merged Partition ids : ", all_partition_ids)

            self._execute_partition_join(join_info, next_join_info, all_partition_ids)

        else :
            self._execute_direct_join(left_table_rows, right_table_rows, join_info, next_join_info)

        return


    def _execute_partition_join(self, join_info, next_join_info, partition_ids):
        next_join_column = next_join_info[0]
        next_join_table = next_join_info[1]

        # Breakdown the join_info
        join_type = join_info['join_type']
        left_table = join_info['left_table']
        right_table = join_info['right_table']
        join_column = join_info['join_column']
        join_column_right = join_info['join_column_right']

        left_alias = join_info['left_alias']
        right_alias = join_info['right_alias']

        left_table_name = left_table
        if (left_alias != None):
            left_table_name = left_alias
        
        right_table_name = right_table
        if (right_alias != None):
            right_table_name = right_alias

        # Get metadata
        left_table_column_names = None
        right_table_column_names = None

        # First join order always get column names from Cassandra [MIGHT BE DELETED]
        if (self.join_order == 1):
            left_table_column_names = get_column_names_from_db(self.session, self.keyspace, left_table)


        else :
            left_table_column_names = self.current_result_column_names

        
        left_table_column_names = self.join_metadata.get_columns_of_table(left_table_name)
        right_table_column_names = self.join_metadata.get_columns_of_table(right_table_name)

        # Add column names to join_info [MIGHT BE DELETED]
        join_info['left_columns'] = left_table_column_names
        join_info['right_columns'] = right_table_column_names

        intermediate_result = IntermediatePartitionedHashResult(join_info, self.max_data_size, next_join_info)
        result_partition_ids = intermediate_result.build_result(partition_ids)

        print("Execute partition join ids : ", result_partition_ids)

        # Save partition_ids of result for next join in case needed
        self.current_join_partition_ids = result_partition_ids

        # Set current result with empty list, indicating all results are in disk
        self.current_result = []

        # Save metadata of column names of left table to self.current_result_column_names
        # new_left_table_column_names = left_table_column_names.union(right_table_column_names)
        # self.current_result_column_names = new_left_table_column_names

        print(f"{join_type} JOIN with order num {self.join_order} completed with Partition Method")
        print(f"Result partitions ID are : {result_partition_ids}\n\n")

        return
    
    
    def _execute_direct_join(self, left_table_rows, right_table_rows, join_info, next_join_info):
        next_join_column = next_join_info[0]
        next_join_table = next_join_info[1]

        session = self.session

        # Breakdown the join_info
        join_type = join_info['join_type']
        left_table = join_info['left_table']
        right_table = join_info['right_table']
        join_column = join_info['join_column']
        join_column_right = join_info['join_column_right']

        left_alias = join_info['left_alias']
        right_alias = join_info['right_alias']

        left_table_name = left_table
        if (left_alias != None):
            left_table_name = left_alias
        
        right_table_name = right_table
        if (right_alias != None):
            right_table_name = right_alias

        # Get metadata
        left_table_column_names = None
        right_table_column_names = None

        # First join order always get column names from Cassandra [MIGHT BE DELETED]
        if (self.join_order == 1):
            left_table_column_names = get_column_names_from_db(self.session, self.keyspace, self.left_table)

        else :
            left_table_column_names = self.current_result_column_names

        left_table_column_names = self.join_metadata.get_columns_of_table(left_table_name)
        right_table_column_names = self.join_metadata.get_columns_of_table(right_table_name)

        # Add column names to join_info [MIGHT BE DELETED]
        join_info['left_columns'] = left_table_column_names
        join_info['right_columns'] = right_table_column_names

        intermediate_result = IntermediateDirectHashResult(join_info, self.max_data_size, next_join_info)

        # Convert result set as list
        left_table_rows = list(left_table_rows)
        right_table_rows = list(right_table_rows)

        print("Left table : ", left_table_rows)
        print("Right table : ", right_table_rows)

        # Set build and probe, build table is smaller
        left_table_size = asizeof.asizeof(left_table_rows)
        right_table_size = asizeof.asizeof(right_table_rows)
        

        # Swap table boolean. Left table is build table by default
        should_swap_table = False

        if (left_table_size > right_table_size):
            # Build table is right table, so swap is mandatory
            should_swap_table = True
            # Below might not be needed
            intermediate_result.swap_build_and_probe()      
            print("BUILD AND PROBE TABLE SWITCHED")      


        if (not should_swap_table): # By default, process left table as Build table
            # Left table insertion to intermediate_result and clear left table rows for each iteration
            # Build table
            for idx in range(len(left_table_rows)):
                row = left_table_rows[idx]
                dict_key = (join_column, left_table_name)
                left_key = row[dict_key]

                # Left table is build table by default
                is_build_table = True
                
                # Use all rows to build hash table
                intermediate_result.add_row_to_intermediate(row, is_build_table)
                left_table_rows[idx] = None

            # Right table insertion to intermediate_result and clear right table rows for each iteration
            # Probe table
            for idx in range(len(right_table_rows)):
                row = right_table_rows[idx]
                dict_key = (join_column_right, right_table_name)
                right_key = row[dict_key]

                # Right table is probe table by default
                is_build_table = False

                # Check if key exists in hash table. Action based on join type
                is_key_exists = intermediate_result.is_key_in_hashtable(right_key)

                if (join_type == "INNER"):
                    if (is_key_exists):
                        intermediate_result.add_row_to_intermediate(row, is_build_table)
                
                elif (join_type == "LEFT_OUTER"):
                    # We don't need non-matching rows from right table
                    if (is_key_exists):
                        intermediate_result.add_row_to_intermediate(row, is_build_table)
                
                elif (join_type == "RIGHT_OUTER"):
                    # We need rows from right table, whether they match or not
                    if (is_key_exists):
                        intermediate_result.add_row_to_intermediate(row, is_build_table)
                    else :
                        intermediate_result.add_row_to_right_nomatch(row)

                else : # join_type == "FULL_OUTER"
                    if (is_key_exists):
                        intermediate_result.add_row_to_intermediate(row, is_build_table)
                    else :
                        intermediate_result.add_row_to_right_nomatch(row)

                right_table_rows[idx] = None

        else : # Process right table first as Build table
            # Right table insertion to intermediate_result and clear right table rows for each iteration
            # Build table
            for idx in range(len(right_table_rows)):
                row = right_table_rows[idx]
                dict_key = (join_column_right, right_table_name)
                right_key = row[dict_key]

                # Now that the tables have been swap, right table is now a Build table
                is_build_table = True

                intermediate_result.add_row_to_intermediate(row, is_build_table)
                right_table_rows[idx] = None

            # Left table insertion to intermediate_result and clear left table rows for each iteration
            # Probe table
            for idx in range(len(left_table_rows)):
                row = left_table_rows[idx]
                dict_key = (join_column, left_table_name)
                left_key = row[dict_key]

                # Left table is build table by default
                is_build_table = False

                # Check if key exists in hash table. Action based on join type
                is_key_exists = intermediate_result.is_key_in_hashtable(left_key)

                if (join_type == "INNER"):
                    if (is_key_exists):
                        intermediate_result.add_row_to_intermediate(row, is_build_table)
                
                elif (join_type == "LEFT_OUTER"):
                    # We need rows from left table, whether they match or not
                    if (is_key_exists):
                        intermediate_result.add_row_to_intermediate(row, is_build_table)
                    else :
                        intermediate_result.add_row_to_left_nomatch(row)
                
                elif (join_type == "RIGHT_OUTER"):
                    # We don't need non-matching rows from right table
                    if (is_key_exists):
                        intermediate_result.add_row_to_intermediate(row, is_build_table)

                else : # join_type == "FULL_OUTER"
                    if (is_key_exists):
                        intermediate_result.add_row_to_intermediate(row, is_build_table)
                    else :
                        intermediate_result.add_row_to_left_nomatch(row)


                left_table_rows[idx] = None



        # Clear both left_table_rows and right_table_rows
        left_table_rows = None
        right_table_rows = None
        
        result, result_partition_ids = intermediate_result.build_result()

        # Append result as current result
        self.current_result = result

        # Save metadata of column names of left table to self.current_result_column_names
        # new_left_table_column_names = left_table_column_names.union(right_table_column_names)
        # self.current_result_column_names = new_left_table_column_names

        print("\n")
        print(f"{join_type} JOIN with order num {self.join_order} completed with direct method")
        print("\n\n")

        return
