import psutil
from pympler import asizeof

from commands import *
from utils import *
from math_utils import *

from cassandra.query import dict_factory
from join_executor import JoinExecutor

class TupleJoinExecutor(JoinExecutor):
    def __init__(self, session, keyspace_name):
        super().__init__(session, keyspace_name)

        self.current_result = []
        
        # Left table partition ids
        self.left_table_last_partition_id = -1
        self.right_table_last_partition_id = -1
        self.result_last_partition_id = -1

        # Maximum size of partition of tuples
        self.partition = []
        self.partition_max_size = megabyte_to_byte(50)

        # Override force partition
        self.force_partition = True


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
                left_table = join_command.left_table
                right_table = join_command.right_table
                join_column = join_command.join_column
                join_column_right = join_command.join_column_right
                join_operator = join_command.join_operator

                if (join_column_right == None): #Join column name is same on both table
                    join_column_right = join_column

                # Checking whether if join column is exist
                table_cols = []

                is_lefttable_in_metadata = self.join_metadata.is_table_exists(left_table)
                is_righttable_in_metadata = self.join_metadata.is_table_exists(right_table)

                check_leftcols_query = f"SELECT * FROM system_schema.columns where keyspace_name = '{self.keyspace}' AND table_name = '{left_table}'"
                check_rightcols_query = f"SELECT * FROM system_schema.columns where keyspace_name = '{self.keyspace}' AND table_name = '{right_table}'"

                if (not is_lefttable_in_metadata):
                    left_meta_rows = session.execute(check_leftcols_query)
                    self.join_metadata.add_table(left_table)

                    for row in left_meta_rows:
                        column_name = row['column_name']
                        self.join_metadata.add_one_column(left_table, column_name)
        
                if (not left_table in self.table_query):
                    self.table_query[left_table] = f"SELECT * FROM {left_table}"

                # Below are action for Right table 
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
                    "left_table" : left_table,
                    "right_table" : right_table,
                    "join_column" : join_column,
                    "join_column_right" : join_column_right,
                    "join_operator" : join_operator
                }

                self.total_join_order += 1

                # To be proceeded later
                self.joins_info.append(curr_join_info)


        # Execute all joins based on self.joins_info
        for join_info_idx in range(len(self.joins_info)):
            join_info = self.joins_info[join_info_idx]

            # Use below data when needed
            join_type = join_info['join_type']
            left_table = join_info['left_table']
            right_table = join_info['right_table']
            join_column = join_info['join_column']
            join_column_right = join_info['join_column_right']
            join_operator = join_info['join_operator']

            # Join preparation has been prepared, now continue to join process
            self._decide_join(join_info)

            # Below, Reset all join info
            # Increment join_order num
            self.join_order += 1

            # Result will be left table for next join
            self.left_table_last_partition_id = self.result_last_partition_id
            self.right_table_last_partition_id = -1
            self.result_last_partition_id = -1

            # DUMMY PRINT
            print("\n\n\n")


        # Build final result here
        # TODO: Change code below for tuple join
        if (self.current_result == []): # Final result is in disk
            # For now, merge all results to memory
            final_result = []
            left_last_partition_id = self.left_table_last_partition_id
            final_join_order = self.join_order

            for id in range(left_last_partition_id):
                # Read data
                partition_data = read_from_partition_nonhash(final_join_order, id, True)

                for row in partition_data:
                    final_result.append(row)

            # TODO: Delete all tmpfiles after join operation

            return final_result
        
        else : # Final result is in memory (self.current_result), return immediately

            return self.current_result


    def _get_left_data(self, left_table):
        # Only get data when fit to memory. Otherwise, load in join execution function
        session = self.session

        left_table_rows = None
        is_data_in_partitions = False
        left_last_partition_id = -1

        # Read data
        if (self.join_order == 1):
            left_table_query = self.table_query[left_table]
            print("Left table query : ", left_table_query)

            # TODO: Use paging for bigger queries
            left_table_rows = session.execute(left_table_query)

            left_table_rows = list(left_table_rows)

            # Change the dict structure, so each column has parent table
            # Also, adding flag to each row
            for idx in range(len(left_table_rows)):
                left_row = left_table_rows[idx]

                row_dict = {}
                for key in left_row:
                    value = left_row[key]
                    new_key = (key, left_table)
                    row_dict[new_key] = value

                left_row = row_dict

                # Save as list. 0 or 1 is a flag to identify whether a particular row has matched or has not.
                left_table_rows[idx] = {
                    "data" : left_row,
                    "flag" : 0
                }
            
        else : # Non first order join, left table may from partitions or cassandra
            if (self.current_result == []):
                print("Masuk disk")
                left_last_partition_id = self.left_table_last_partition_id
                is_data_in_partitions = True

                return left_table_rows, is_data_in_partitions, left_last_partition_id
                
            
            else :
                print("Masuk memory")
                left_table_rows = self.current_result

                # Reset current result
                self.current_result = []


        # SIZE CHECKING
        if ((self.max_data_size <= asizeof.asizeof(left_table_rows)) or self.force_partition):
            is_data_in_partitions = True

            left_last_partition_id = put_into_partition_nonhash(left_table_rows, self.join_order, self.partition_max_size, self.left_table_last_partition_id, True)
            # Update last partition id
            self.left_table_last_partition_id = left_last_partition_id

        return left_table_rows, is_data_in_partitions, left_last_partition_id


    def _get_right_data(self, right_table):
        # Only get data when fit to memory. Otherwise, load in join execution function
        session = self.session

        right_table_rows = None
        is_data_in_partitions = False
        right_last_partition_id = -1

        right_table_query = self.table_query[right_table]
        print("Right table query : ", right_table_query)

        # TODO: Use paging query
        right_table_rows = session.execute(right_table_query)
        right_table_rows = list(right_table_rows)

        # Adding flag for outer joins
        for idx in range(len(right_table_rows)):
            right_row = right_table_rows[idx]
            row_dict = {}

            for key in right_row:
                value = right_row[key]
                new_key = (key, right_table)
                row_dict[new_key] = value

            right_row = row_dict
            right_table_rows[idx] = {
                "data" : right_row,
                "flag" : 0
            }

        # SIZE CHECKING
        if ((self.max_data_size <= asizeof.asizeof(right_table_rows)) or self.force_partition):
            is_data_in_partitions = True

            right_last_partition_id = put_into_partition_nonhash(right_table_rows, self.join_order, self.partition_max_size, self.right_table_last_partition_id, False)
            # Update partition last id
            self.right_table_last_partition_id = right_last_partition_id


        return right_table_rows, is_data_in_partitions, right_last_partition_id


    def _decide_join(self, join_info):
        left_table = join_info['left_table']
        right_table = join_info['right_table']
        join_order = join_info['join_order']
        next_join_order = join_order + 1

        # Left table
        left_table_rows, is_left_in_partitions, left_last_partition_id = self._get_left_data(left_table)

        # Right table
        right_table_rows, is_right_in_partitions, right_last_partition_id = self._get_right_data(right_table)

        # Check size
        if ((not is_left_in_partitions) and (not is_right_in_partitions)):
            if (self.max_data_size <= asizeof.asizeof(left_table_rows) + asizeof.asizeof(right_table_rows)):
                # Flush both tables and update last partition ids
                left_last_partition_id = put_into_partition_nonhash(left_table_rows, self.join_order, self.partition_max_size, self.left_table_last_partition_id, True)
                right_last_partition_id = put_into_partition_nonhash(right_table_rows, self.join_order, self.partition_max_size, self.right_table_last_partition_id, False)

                self.left_table_last_partition_id = left_last_partition_id
                self.right_table_last_partition_id = right_last_partition_id

                is_left_in_partitions = True
                is_right_in_partitions = True

        # Do join based on whether data is partitioned. There are 4 possibilities
        if (is_left_in_partitions):
            if (is_right_in_partitions):
                self._execute_both_partition(join_info)
            else :
                self._execute_left_partition(join_info, right_table_rows)

        else: # Left data is in memory
            if (is_right_in_partitions):
                self._execute_right_partition(join_info, left_table_rows)
            else :
                self._execute_both_direct(join_info, left_table_rows, right_table_rows)

        # Flush if self.partition is not empty
        if (self.partition != []):
            result_last_partition_id = put_into_partition_nonhash(self.partition, next_join_order, self.partition_max_size, self.result_last_partition_id, True)
            # Update result last partition id and empty the flushed partition
            self.result_last_partition_id = result_last_partition_id
            self.partition = []

        return


    def _execute_both_partition(self, join_info):
        for left_partition_id in range(self.left_table_last_partition_id+1):
            left_table_rows = read_from_partition_nonhash(self.join_order, left_partition_id, True)

            
            for right_partition_id in range(self.right_table_last_partition_id+1):
                right_table_rows = read_from_partition_nonhash(self.join_order, right_partition_id, False)

                for left_row in left_table_rows:
                    left_row_data = left_row["data"]
                    for right_row in right_table_rows:
                        right_row_data = right_row["data"]
                        # Do join based on join type and join condition
                        merged_row = self._merge_row(join_info, left_row_data, right_row_data)

                        if (merged_row == None):
                            continue

                        # Current result
                        join_order = join_info['join_order']
                        self._result_handler(merged_row, join_order)

                        # Update the flag for both left row and right row
                        left_row["flag"] = 1
                        right_row["flag"] = 1
                        

                # Update right partition in local
                update_partition_nonhash(right_table_rows, self.join_order, right_partition_id, False)


            # Update left partition in local
            update_partition_nonhash(left_table_rows, self.join_order, left_partition_id, True)


                # Flush the no matched rows for outer joins
        join_type = join_info['join_type']
        if (join_type == "LEFT_OUTER"):
            self._flush_left_in_partitions(join_info)

        elif (join_type == "RIGHT_OUTER"):
            self._flush_right_in_partition(join_info)

        elif (join_type == "FULL_OUTER"):
            self._flush_left_in_partitions(join_info)
            self._flush_right_in_partition(join_info)

        return


    def _execute_both_direct(self, join_info, left_table_rows, right_table_rows):
        # print("Left rows : ", left_table_rows, "\n")
        # print("Right rows : ", right_table_rows)

        for left_row in left_table_rows:
            left_row_data = left_row["data"]
            for right_row in right_table_rows:
                right_row_data = right_row["data"]
                # Do join based on join type and join condition
                merged_row = self._merge_row(join_info, left_row_data, right_row_data)

                if (merged_row == None):
                    continue

                # Current result
                join_order = join_info['join_order']
                self._result_handler(merged_row, join_order)

                # Update the flag
                left_row["flag"] = 1
                right_row["flag"] = 1
                
        # Flush the no matched rows for outer joins
        join_type = join_info['join_type']
        if (join_type == "LEFT_OUTER"):
            self._flush_left_in_memory(join_info, left_table_rows)

        elif (join_type == "RIGHT_OUTER"):
            self._flush_right_in_memory(join_info, right_table_rows)

        elif (join_type == "FULL_OUTER"):
            self._flush_left_in_memory(join_info, left_table_rows)
            self._flush_right_in_memory(join_info, right_table_rows)


        return

    def _execute_right_partition(self, join_info, left_table_rows):
        for left_row in left_table_rows:
            left_row_data = left_row["data"]

            for right_partition_id in range(self.right_table_last_partition_id+1):
                right_table_rows = read_from_partition_nonhash(self.join_order, right_partition_id, False)

                for right_row in right_table_rows:
                    right_row_data = right_row["data"]
                    # Do join based on join type and join condition
                    merged_row = self._merge_row(join_info, left_row_data, right_row_data)

                    if (merged_row == None):
                        continue

                    # Current result
                    join_order = join_info['join_order']
                    self._result_handler(merged_row, join_order)

                    # Update the flag
                    left_row["flag"] = 1
                    right_row["flag"] = 1


                # Update right partition in local
                update_partition_nonhash(right_table_rows, self.join_order, right_partition_id, False)
        
        # Flush the no matched rows for outer joins
        join_type = join_info['join_type']
        if (join_type == "LEFT_OUTER"):
            self._flush_left_in_memory(join_info, left_table_rows)

        elif (join_type == "RIGHT_OUTER"):
            self._flush_right_in_partition(join_info)

        elif (join_type == "FULL_OUTER"):
            self._flush_left_in_memory(join_info, left_table_rows)
            self._flush_right_in_partition(join_info)

        return


    def _execute_left_partition(self, join_info, right_table_rows):
        for left_partition_id in range(self.left_table_last_partition_id+1):
            left_table_rows = read_from_partition_nonhash(self.join_order, left_partition_id, True)

            for left_row in left_table_rows:
                left_row_data = left_row["data"]
                for right_row in right_table_rows:
                    right_row_data = right_row["data"]
                    # Do join based on join type and join condition
                    merged_row = self._merge_row(join_info, left_row_data, right_row_data)

                    if (merged_row == None):
                        continue

                    # Current result
                    join_order = join_info['join_order']
                    self._result_handler(merged_row, join_order)

                    # Update the flag
                    left_row["flag"] = 1
                    right_row["flag"] = 1
                    
        
            # Update left partition in local
            update_partition_nonhash(left_table_rows, self.join_order, left_partition_id, True)

        # Flush the no matched rows for outer joins
        join_type = join_info['join_type']
        if (join_type == "LEFT_OUTER"):
            self._flush_left_in_partitions(join_info)

        elif (join_type == "RIGHT_OUTER"):
            self._flush_left_in_memory(join_info, right_table_rows)

        elif (join_type == "FULL_OUTER"):
            self._flush_left_in_partitions(join_info)
            self._flush_right_in_memory(join_info, right_table_rows)

        return


    def _merge_row(self, join_info, left_data, right_data):
        should_do_join = False

        left_table = join_info['left_table']
        right_table = join_info['right_table']
        join_column = join_info['join_column']
        join_column_right = join_info['join_column_right']
        

        operator = join_info['join_operator']

        # Create key
        left_key = (join_column, left_table)
        right_key = (join_column_right, right_table)

        # Get data based on key
        left_join_column_data = left_data[left_key]
        right_join_column_data = right_data[right_key]

        try:
            left_join_column_data = float(left_join_column_data)
            right_join_column_data = float(right_join_column_data)
        
        except :
            pass

        if (operator == "="):
            if (left_join_column_data == right_join_column_data):
                should_do_join = True

        elif (operator == "<"):
            if (left_join_column_data < right_join_column_data):
                should_do_join = True

        elif (operator == ">"):
            if (left_join_column_data > right_join_column_data):
                should_do_join = True

        elif (operator == "<="):
            if (left_join_column_data <= right_join_column_data):
                should_do_join = True

        elif (operator == ">="):
            if (left_join_column_data >= right_join_column_data):
                should_do_join = True

        elif (operator == "!="):
            if (left_join_column_data != right_join_column_data):
                should_do_join = True

        else :
            # Wrong operator
            return None
        
        if (not should_do_join):
            return None
        
        # Do the row join here
        # merged_row_data = dict(list(left_data.items()) + list(right_data.items()))
        # TODO: Do the join based on the newest row structure
        merged_row_data = {}

        for key in left_data:
            merged_row_data[key] = left_data[key]

        for key in right_data:
            merged_row_data[key] = right_data[key]

        merged_row = {"data" : merged_row_data, "flag" : 0}

        return merged_row


    def _force_merge_row(self, left_data, right_data):
        merged_row_data = {}

        for key in left_data:
            merged_row_data[key] = left_data[key]

        for key in right_data:
            merged_row_data[key] = right_data[key]

        merged_row = {"data" : merged_row_data, "flag" : 0}

        return merged_row


    def _result_handler(self, merged_row, join_order):

        next_join_order = join_order + 1
        if (self.result_last_partition_id == -1):
            # Add current result to self.current_result first
            self.current_result.append(merged_row)

            # If size of object is too big, flush result
            if (self._get_size() >= self.max_data_size):
                result_last_partition_id = put_into_partition_nonhash(self.current_result, next_join_order, self.partition_max_size, self.result_last_partition_id, True)

                # Update on result partition id and empty the current result
                self.result_last_partition_id = result_last_partition_id
                self.current_result = []

        else : # Some result is in partition
            # Add to self.partition then flush when it is big enough
            self.partition.append(merged_row)
            
            if (asizeof.asizeof(self.partition) >= self.partition_max_size):
                result_last_partition_id = put_into_partition_nonhash(self.partition, next_join_order, self.partition_max_size, self.result_last_partition_id, True)
                self.result_last_partition_id = result_last_partition_id

        return


    def _flush_left_in_memory(self, join_info, left_table_rows):
        join_order = join_info['join_order']
        for idx in range(len(left_table_rows)):
            left_row = left_table_rows[idx]
            left_data = left_row["data"]
            flag = left_row["flag"]

            if (flag == 1 or flag == "1"): # Row has matched
                continue
            
            right_table = join_info['right_table']
            right_table_columnns = self.join_metadata.get_columns_of_table(right_table)
            right_data = construct_null_columns(right_table, right_table_columnns)

            merged_row = self._force_merge_row(left_data, right_data)
            self._result_handler(merged_row, join_order)

        return

    def _flush_right_in_memory(self, join_info, right_table_rows):
        join_order = join_info['join_order']
        for idx in range(len(right_table_rows)):
            right_row = right_table_rows[idx]
            right_data = right_row["data"]
            flag = right_row["flag"]

            if (flag == 1 or flag == "1"):
                continue

            left_table = join_info['left_table']
            left_table_columns = self.join_metadata.get_columns_of_table(left_table)
            left_data = construct_null_columns(left_table, left_table_columns)

            merged_row = self._force_merge_row(left_data, right_data)
            self._result_handler(merged_row, join_order)

        return
    
    def _flush_left_in_partitions(self, join_info):
        join_order = join_info['join_order']
        for left_partition_id in range(self.left_table_last_partition_id+1):
            left_table_rows = read_from_partition_nonhash(self.join_order, left_partition_id, True)

            self._flush_left_in_memory(join_info, left_table_rows)

        return

    def _flush_right_in_partition(self, join_info):
        join_order = join_info['join_order']
        for right_partition_id in range(self.right_table_last_partition_id+1):
            right_table_rows = read_from_partition_nonhash(self.join_order, right_partition_id, False)

            self._flush_right_in_memory(join_info, right_table_rows)

        return


    def _get_size(self):
        return asizeof.asizeof(self)
