import psutil
import os
from pympler import asizeof

from commands import *
from utils import *
from math_utils import *

from cassandra.query import dict_factory, SimpleStatement
from join_executor import JoinExecutor

class NestedJoinExecutor(JoinExecutor):
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
        self.force_partition = False

        # To force save partition trace
        self.save_partition_trace = True

        # FOR TESTING USAGE
        self.max_data_size = 0


    def execute(self):
        # Consume the commands queue, execute join
        session = self.session
        selects_valid = None

        for command in self.command_queue :
            command_type = command.type

            if (command_type == "SELECT"):
                table = command.table
                columns = command.columns

                if (not table in self.selected_cols):
                    self.selected_cols[table] = columns

                else :
                    self.selected_cols[table] = columns.union(self.selected_cols[table])    

            elif (command_type == "JOIN"):

                if (selects_valid == None):
                    selects_valid = self.selects_validation()

                if (not selects_valid):
                    print("Some join columns are not selected!")
                    return []

                join_command = command
                
                # Define join variables
                join_type = join_command.join_type
                real_left_table = join_command.left_table
                real_right_table = join_command.right_table
                join_column = join_command.join_column
                join_column_right = join_command.join_column_right
                join_operator = join_command.join_operator

                left_alias = join_command.left_alias
                right_alias = join_command.right_alias


                if (join_column_right == None): #Join column name is same on both table
                    join_column_right = join_column

                left_table = real_left_table
                if (left_alias != None):
                    left_table = left_alias

                right_table = real_right_table
                if (right_alias != None):
                    right_table = right_alias

                # Checking whether if join column is exist
                table_cols = []

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
                    if (left_table in self.selected_cols):
                        query = f"SELECT "
                        selected_table_cols = list(self.selected_cols[left_table])

                        for idx in range(len(selected_table_cols)):
                            col = selected_table_cols[idx]
                            if (idx == len(selected_table_cols) - 1):
                                query += f"{col} "
                            else :    
                                query += f"{col}, "

                        query += f" FROM {real_left_table}"

                        self.table_query[left_table] = query

                    else :
                        self.table_query[left_table] = f"SELECT * FROM {real_left_table}"

                # Below are actions for Right table 
                if (not is_righttable_in_metadata):
                    right_meta_rows = session.execute(check_rightcols_query)
                    self.join_metadata.add_table(right_table)

                    for row in right_meta_rows:
                        column_name = row['column_name']
                        self.join_metadata.add_one_column(right_table, column_name)

                # No join columns exception here
                table_cols = self.join_metadata.get_columns_of_table(right_table)
                if not(join_column_right in table_cols):
                    # Throw error
                    print("Join column is not in right-table")


                # Build select query
                select_query = "SELECT"
                if (right_table in self.selected_cols):
                    selected_cols = list(self.selected_cols[right_table])
                    for idx in range(len(selected_cols)):
                        col = selected_cols[idx]

                        if (idx == len(selected_cols) - 1):
                            select_query += f" {col} FROM {real_right_table}"
                        
                        else :
                            select_query += f" {col},"



                else :
                    for idx in range(len(table_cols)):
                        col = table_cols[idx]

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
                    "left_table" : left_table,
                    "right_table" : right_table,
                    "join_column" : join_column,
                    "join_column_right" : join_column_right,
                    "join_operator" : join_operator,
                    "left_alias" : left_alias,
                    "right_alias" : right_alias
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


        # Build final result here
        # if (self.current_result == []): # Final result is in disk
        #     # For now, merge all results to memory
        #     final_result = []
        #     left_last_partition_id = self.left_table_last_partition_id
        #     final_join_order = self.join_order


        #     for id in range(0,left_last_partition_id+1):
        #         # Read data
        #         partition_data = read_from_partition_nonhash(final_join_order, id, True)

        #         for row in partition_data:
        #             final_result.append(row)

        #     # TODO: Delete all tmpfiles after join operation

        #     return final_result
        
        # else : # Final result is in memory (self.current_result), return immediately

        #     return self.current_result
        return self


    def _get_left_data(self, left_table, left_alias):
        # Only get data when fit to memory. Otherwise, load in join execution function
        session = self.session

        left_table_rows = None
        is_data_in_partitions = False
        left_last_partition_id = -1

        left_table_name = left_table
        if (left_alias != None):
            left_table_name = left_alias
        

        # Read data
        if (self.join_order == 1):
            left_table_query = self.table_query[left_table_name]
            print("Left table query : ", left_table_query)

            left_table_rows = []
            statement = SimpleStatement(left_table_query, fetch_size=self.cassandra_fetch_size)
            results = session.execute(statement)
            
            if (not results.has_more_pages):
                left_table_rows = list(results)
                for idx in range(len(left_table_rows)):
                    # Change the dict structure, so each column has parent table
                    # Also, adding flag to each row
                    left_row = left_table_rows[idx]

                    row_dict = {}
                    for key in left_row:
                        value = left_row[key]
                        new_key = (key, left_table_name)
                        row_dict[new_key] = value

                    left_row = row_dict
                    left_table_rows[idx] = {
                        "data" : left_row,
                        "flag" : 0
                    }

                self.paging_state[left_table_name] = None

                # Size checking is not conducted with assumption
                # 5000 rows always fit in memory
            
            else :
                # Handle rows in the first page
                rows_fetched = 0
                for row in results:
                    # Change the dict structure, so each column has parent table
                    # Also, adding flag to each row
                    left_row = row

                    row_dict = {}
                    for key in left_row:
                        value = left_row[key]
                        new_key = (key, left_table_name)
                        row_dict[new_key] = value

                    left_row = row_dict

                    # Save as list. 0 or 1 is a flag to identify whether a particular row has matched or has not.
                    left_table_rows.append({
                        "data" : left_row,
                        "flag" : 0
                    })
                    rows_fetched += 1

                    if (rows_fetched == self.cassandra_fetch_size):
                        self.paging_state[left_table_name] = results.paging_state
                        break
                
                # Handle the rest of the rows
                while (results.has_more_pages):
                    rows_fetched = 0
                    ps = self.paging_state[left_table_name]

                    # Fetch based on last paging state
                    statement = SimpleStatement(left_table_query, fetch_size=self.cassandra_fetch_size)
                    results = session.execute(statement, paging_state = ps)

                    for row in results:
                        # Change the dict structure, so each column has parent table
                        # Also, adding flag to each row
                        left_row = row

                        row_dict = {}
                        for key in left_row:
                            value = left_row[key]
                            new_key = (key, left_table_name)
                            row_dict[new_key] = value

                        left_row = row_dict

                        # Save as list. 0 or 1 is a flag to identify whether a particular row has matched or has not.
                        left_table_rows.append({
                            "data" : left_row,
                            "flag" : 0
                        })

                        rows_fetched += 1
                        if (rows_fetched == self.cassandra_fetch_size):
                            self.paging_state[left_table_name] = results.paging_state
                            break
                    
                    # SIZE Checking: Checking done per page
                    if ((self.max_data_size <= self.get_size()) or is_data_in_partitions or self.force_partition):
                        is_data_in_partitions = True

                        left_last_partition_id = put_into_partition_nonhash(left_table_rows, self.join_order, self.partition_max_size, self.left_table_last_partition_id, True)
                        # Reset left_table_rows to empty list
                        left_table_rows = []
                        
                        # Update last partition id
                        self.left_table_last_partition_id = left_last_partition_id

            
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

        return left_table_rows, is_data_in_partitions, left_last_partition_id


    def _get_right_data(self, right_table, right_alias):
        # Only get data when fit to memory. Otherwise, load in join execution function
        session = self.session

        right_table_rows = None
        is_data_in_partitions = False
        right_last_partition_id = -1

        right_table_name = right_table
        if (right_alias != None):
            right_table_name = right_alias

        right_table_query = self.table_query[right_table_name]
        print("Right table query : ", right_table_query)

        right_table_rows = []
        statement = SimpleStatement(right_table_query, fetch_size=self.cassandra_fetch_size)
        results = session.execute(statement)

        if (not results.has_more_pages):
            right_table_rows = list(results)
            for idx in range(len(right_table_rows)):
                # Change dict structure and add flag
                right_row = right_table_rows[idx]
                row_dict = {}

                for key in right_row:
                    value = right_row[key]
                    new_key = (key, right_table_name)
                    row_dict[new_key] = value

                right_row = row_dict
                right_table_rows[idx] = {
                    "data" : right_row,
                    "flag" : 0
                }

            self.paging_state[right_table_name] = None

            # Size checking is not conducted with assumption
            # 5000 rows always fit in memory

        else :
            # Handle rows in first page
            rows_fetched = 0
            for row in results:
                # Change dict structure and add flag
                right_row = row
                row_dict = {}

                for key in right_row:
                    value = right_row[key]
                    new_key = (key, right_table_name)
                    row_dict[new_key] = value

                right_row = row_dict
                right_table_rows.append({
                    "data" : right_row,
                    "flag" : 0
                })
                rows_fetched += 1

                if (rows_fetched == self.cassandra_fetch_size):
                    self.paging_state[right_table_name] = results.paging_state
                    break

            # Handle the rest of the rows
            while (results.has_more_pages):
                rows_fetched = 0
                ps = self.paging_state[right_table_name]

                # Fetch based on last paging state
                statement = SimpleStatement(right_table_query, fetch_size=self.cassandra_fetch_size)
                results = session.execute(statement, paging_state = ps)

                for row in results:
                    # Change the dict structure, so each column has parent table
                    # Also, adding flag to each row
                    right_row = row

                    row_dict = {}
                    for key in right_row:
                        value = right_row[key]
                        new_key = (key, right_table_name)
                        row_dict[new_key] = value

                    right_row = row_dict

                    # Save as list. 0 or 1 is a flag to identify whether a particular row has matched or has not.
                    right_table_rows.append({
                        "data" : right_row,
                        "flag" : 0
                    })

                    rows_fetched += 1
                    if (rows_fetched == self.cassandra_fetch_size):
                        self.paging_state[right_table_name] = results.paging_state
                        break
                
                # SIZE Checking: Check size used per page
                if ((self.max_data_size <= self.get_size()) or is_data_in_partitions or self.force_partition):
                    is_data_in_partitions = True

                    right_last_partition_id = put_into_partition_nonhash(right_table_rows, self.join_order, self.partition_max_size, self.right_table_last_partition_id, False)
                    # Reset left_table_rows to empty list
                    right_table_rows = []
                    
                    # Update last partition id
                    self.right_table_last_partition_id = right_last_partition_id


        return right_table_rows, is_data_in_partitions, right_last_partition_id


    def _decide_join(self, join_info):
        left_table = join_info['left_table']
        right_table = join_info['right_table']
        join_order = join_info['join_order']
        next_join_order = join_order + 1

        left_alias = join_info['left_alias']
        right_alias = join_info['right_alias']

        left_table_name = left_table
        if (left_alias != None):
            left_table_name = left_alias
        
        right_table_name = right_table
        if (right_alias != None):
            right_table_name = right_alias

        # Left table
        left_table_rows, is_left_in_partitions, left_last_partition_id = self._get_left_data(left_table, left_alias)

        # Right table
        right_table_rows, is_right_in_partitions, right_last_partition_id = self._get_right_data(right_table, right_alias)

        print(f"Left in partition : {is_left_in_partitions}")
        print(f"Left rows : {left_table_rows}")
        print(f"Right in partition : {is_right_in_partitions}")
        print(f"Right rows : {right_table_rows}")


        # Do join based on whether data is partitioned. There are 4 possibilities
        if (is_left_in_partitions):
            if (is_right_in_partitions):
                print(f"Join order {join_order} using execute both partition")
                self._execute_both_partition(join_info)
            else :
                print(f"Join order {join_order} using left in partition")
                self._execute_left_partition(join_info, right_table_rows)

        else: # Left data is in memory
            if (is_right_in_partitions):
                print(f"Join order {join_order} using right in partition")
                self._execute_right_partition(join_info, left_table_rows)
            else :
                print(f"Join order {join_order} using both in direct")
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
        left_alias = join_info['left_alias']
        right_alias = join_info['right_alias']

        left_table_name = left_table
        if (left_alias != None):
            left_table_name = left_alias
        
        right_table_name = right_table
        if (right_alias != None):
            right_table_name = right_alias
        

        operator = join_info['join_operator']

        # Create key
        left_key = (join_column, left_table_name)
        right_key = (join_column_right, right_table_name)

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
            if (self.get_size() >= self.max_data_size):
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
            right_alias = join_info['right_alias']

            right_table_name = right_table
            if (right_alias != None):
                right_table_name = right_alias

            right_table_columnns = self.join_metadata.get_columns_of_table(right_table_name)
            right_data = construct_null_columns(right_table_name, right_table_columnns)

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
            left_alias = join_info['left_alias']

            left_table_name = left_table
            if (left_alias != None):
                left_table_name = left_alias

            left_table_columns = self.join_metadata.get_columns_of_table(left_table_name)
            left_data = construct_null_columns(left_table_name, left_table_columns)

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
    
    def save_result(self, filename):
        cwd = os.getcwd()
        filename = filename + ".txt"
        result_folder = os.path.join(cwd, "results")
        if (not os.path.isdir(result_folder)):
            os.mkdir(result_folder)

        result_file_path = os.path.join(result_folder, filename)
        f_res = open(result_file_path, mode='a')

        if (self.current_result == []): # Final result is in disk
            # For now, merge all results to memory
            final_result = []
            left_last_partition_id = self.left_table_last_partition_id
            final_join_order = self.join_order

            tmp_path = os.path.join(cwd, 'tmpfolder')
            print(tmp_path)
            final_res_path = os.path.join(tmp_path, str(final_join_order))

            for id in range(0,left_last_partition_id+1):
                partition_name = f"{id}_l.txt"
                partition_path = os.path.join(final_res_path, partition_name)
                partition_file = open(partition_path, mode='r')
                partition_lines = partition_file.readlines()

                for line in partition_lines:
                    f_res.write(line)

                partition_file.close()

            # TODO: Delete all tmpfiles after join operation

        
        else : # Final result is in memory (self.current_result), return immediately
            
            self.current_result = jsonTupleKeyEncoder(self.current_result)

            for row in self.current_result:
                f_res.write(json.dumps(row)+"\n")

        f_res.close()
        return



