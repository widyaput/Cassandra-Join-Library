import psutil
import os
import time
from pympler import asizeof

from cassandra_joinlib.commands import *
from cassandra_joinlib.utils import *
from cassandra_joinlib.math_utils import *

from cassandra.query import dict_factory, SimpleStatement
from cassandra.concurrent import execute_concurrent
from cassandra_joinlib.join_executor import JoinExecutor

from datetime import datetime

class NestedJoinExecutor(JoinExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.current_result = []
        self.current_result_size = 0
        
        # Left table partition ids
        self.left_table_last_partition_id = -1
        self.right_table_last_partition_id = -1
        self.result_last_partition_id = -1

        # Maximum size of partition of tuples
        self.partition = []
        self.partition_curr_size = 0
        self.partition_max_size = megabyte_to_byte(25)

        # Override force partition
        self.force_partition = False

        # To force save partition trace
        self.save_partition_trace = False

        # FOR TESTING USAGE
        # self.max_data_size = 0
        self.filter_conditions: List[Condition] = []


    def get_left_size(self):
        return self.left_data_size

    def get_right_size(self):
        return self.right_data_size

    def get_result_size(self):
        return self.current_result_size

    def get_data_size(self):
        return self.left_data_size + self.right_data_size + self.current_result_size


    def execute(self, save_as=f"{datetime.now()}"):
        # Consume the commands queue, execute join
        if self.amqp_url:
            super().execute(save_as=save_as)
        if self.session is None:
            return self
        initial_time = time.time()
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

            elif (isinstance(command, FilterCommands)):
                def addColumn(condition: Condition):
                    if (condition.is_base()):
                        if not isinstance(condition.lhs, Condition) and not isinstance(condition.rhs, Condition):
                            if (isinstance(condition.lhs, str)):
                                table_name, column = condition.lhs.split('.')
                                if self.join_metadata.is_table_exists(table_name) \
                                    and self.join_metadata.is_column_exists(table_name, column):
                                        found = False
                                        for info in self.joins_info:
                                            if (
                                                (
                                                    info["left_table"] == table_name and
                                                    info["join_column"] == column
                                                ) or (
                                                    info["right_table"] == table_name and
                                                    info["join_column_right"] == column
                                                    )
                                                ):
                                                found = True
                                                break
                                        if not found and not "*" in self.table_query[table_name]:
                                            query = self.table_query[table_name]
                                            if not(column in query):
                                                idx = query.index("FROM") 
                                                self.table_query[table_name] = query[:idx] + ", " + column + " " + query[idx:]
                    else:
                        addColumn(condition.lhs)
                        if condition.rhs is not None:
                            addColumn(condition.rhs)
                    
                def parseFilter(condition: Condition):
                    if (condition.is_base()):
                        if not isinstance(condition.lhs, Condition) and not isinstance(condition.rhs, Condition):
                            if (isinstance(condition.lhs, str)):
                                table_name, column = condition.lhs.split('.')
                                if self.join_metadata.is_table_exists(table_name) \
                                    and self.join_metadata.is_column_exists(table_name, column):
                                        found = False
                                        self.table_query[table_name] = self.table_query[table_name].removesuffix('ALLOW FILTERING')
                                        for info in self.joins_info:
                                            if (
                                                (
                                                    info["left_table"] == table_name and
                                                    info["join_column"] == column
                                                ) or (
                                                    info["right_table"] == table_name and
                                                    info["join_column_right"] == column
                                                    )
                                                ):
                                                if not ("where" in self.table_query[table_name]):
                                                    self.table_query[table_name] += " where "
                                                else:
                                                    self.table_query[table_name] += " or "
                                                if not (f"{column} = " in self.table_query[table_name]):
                                                    if isinstance(condition.rhs, str):
                                                        self.table_query[table_name] += f"{column} {condition.operator} '{str(condition.rhs)}'"
                                                    else:
                                                        self.table_query[table_name] += f"{column} {condition.operator} {str(condition.rhs)}"
                                                else:
                                                    print("cannot query with 2 or more equal")
                                                found = True

                                        if not found and not "*" in self.table_query[table_name]:
                                            query = self.table_query[table_name]
                                            if not(column in query):
                                                idx = query.index("FROM") 
                                                self.table_query[table_name] = query[:idx] + ", " + column + " " + query[idx:]
                                        
                                        self.table_query[table_name] += " ALLOW FILTERING"
                        
                    else:
                        if condition.is_always_and():
                            parseFilter(condition.lhs)
                            parseFilter(condition.rhs)
                        else:
                            addColumn(condition.lhs)
                            if condition.rhs is not None:
                                addColumn(condition.rhs)
                        
                parseFilter(command.expressions)
                self.filter_conditions.append(command.expressions)
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
                        if row['kind'] == 'partition_key':
                            self.join_metadata.add_pk_column(left_table, column_name)
        
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
                        if row['kind'] == 'partition_key':
                            self.join_metadata.add_pk_column(right_table, column_name)

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
        initial_join_time = time.time()
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

            # Delete previous join order
            if (not self.save_partition_trace):
                delete_prev_result(str(self.join_order - 1))

        final_join_time = time.time()
        final_time = time.time()

        self.time_elapsed['join'] = final_join_time - initial_join_time
        self.time_elapsed['total'] = final_time - initial_time

        if save_as:
            if self.nodes_order == -1:
                self.__save_result(save_as)
            else:
                self.__save_result(save_as + "_" + str(self.nodes_order))

        return self


    def _get_left_data(self, left_table, left_alias):
        # Only get data when fit to memory. Otherwise, load in join execution function
        session = self.session
        assert(session is not None)

        left_table_rows = []
        is_data_in_partitions = False
        left_last_partition_id = -1

        left_table_name = left_table
        if (left_alias != None):
            left_table_name = left_alias
        

        # Read data
        if (self.join_order == 1):
            fetch_size = self.cassandra_fetch_size
            statement_and_params = []
            if len(self.token_ranges):
                base_left_query = self.table_query[left_table_name].removesuffix('ALLOW FILTERING')
                pks_str = self.join_metadata.get_pk_columns_string_of_table(left_table_name)
                stmt1_str = ""
                stmt2_str = ""
                stmt3_str = ""
                idx_where = base_left_query.find("where")
                if idx_where != -1 :
                    stmt1_str = base_left_query[:idx_where + len("where")] + f" token({pks_str}) > ? AND token({pks_str}) < ? AND" \
                            + base_left_query[idx_where + len("where"):] + " ALLOW FILTERING"
                    stmt2_str = base_left_query[:idx_where + len("where")] + f" token({pks_str}) > ? AND" \
                            + base_left_query[idx_where + len("where"):] + " ALLOW FILTERING"
                    stmt3_str = base_left_query[:idx_where + len("where")] + f" token({pks_str}) < ? AND" \
                            + base_left_query[idx_where + len("where"):] + " ALLOW FILTERING"
                else:
                    stmt1_str = base_left_query + f" where token({pks_str}) > ? AND token({pks_str}) < ? ALLOW FILTERING"
                    stmt2_str = base_left_query + f" where token({pks_str}) > ? ALLOW FILTERING"
                    stmt3_str = base_left_query + f" where token({pks_str}) < ? ALLOW FILTERING"
                stmt1 =  session.prepare(stmt1_str)
                stmt1.fetch_size = fetch_size
                stmt2 =  session.prepare(stmt2_str)
                stmt2.fetch_size = fetch_size
                stmt3 =  session.prepare(stmt3_str)
                stmt3.fetch_size = fetch_size
                for token in self.token_ranges:
                    condition = token.toCondition(pks_str)
                    if condition.is_always_and():
                        params = (int(condition.lhs.rhs), int(condition.rhs.rhs))
                        statement_and_params.append((stmt1, params))
                    else:
                        params1 = (int(condition.lhs.rhs), )
                        params2 = (int(condition.rhs.rhs), )
                        statement_and_params.append((stmt2, params1))
                        statement_and_params.append((stmt3, params2))
                
            else:
                # left_table_queries.append(self.table_query[left_table_name])
                statement_and_params.append((self.table_query[left_table_name], None))
                    
            # print("Left-Table query : ", left_table_queries)
            protocol_version = session.cluster.protocol_version
            concurrency = 500 if protocol_version > 2 else 100
            results = execute_concurrent(session, statement_and_params, concurrency=concurrency, results_generator=True, raise_on_first_error=False)
            for (success, result) in results:
                if not success:
                    print(result)
                else:
                    for row in result:
                        left_row = row
                        tupled_key_dict = {}
                        for key in left_row:
                            value = left_row[key]
                            new_key = (key, left_table_name)
                            tupled_key_dict[new_key] = value
                        
                        left_table_rows.append(
                            {
                                "data": tupled_key_dict,
                                "flag": 0
                            })
                        self.left_data_size += asizeof.asizeof(row)
                        if ((self.max_data_size <= self.get_data_size()) or is_data_in_partitions or self.force_partition): 
                            print("Left data is too big. Put into partition")
                            is_data_in_partitions = True

                            left_last_partition_id = put_into_partition_nonhash(left_table_rows, self.join_order, self.partition_max_size, self.left_table_last_partition_id, True)
                            # Reset left_table_rows to empty list
                            left_table_rows = []
                            self.left_data_size = 0
                            
                            # Update last partition id
                            self.left_table_last_partition_id = left_last_partition_id
            # left_table_query = self.table_query[left_table_name]
            # print("Left table query : ", left_table_query)
            #
            # left_table_rows = []
            # fetch_size = self.cassandra_fetch_size
            # if self.nodes_order != -1:
            #     fetch_size = self.leftest_table_size // self.workers_count
            #     
            # statement = SimpleStatement(left_table_query, fetch_size=fetch_size)
            # results = session.execute(statement)
            # 
            # if (not results.has_more_pages):
            #     left_table_rows = list(results)
            #     for idx in range(len(left_table_rows)):
            #         # Change the dict structure, so each column has parent table
            #         # Also, adding flag to each row
            #         left_row = left_table_rows[idx]
            #
            #         row_dict = {}
            #         for key in left_row:
            #             value = left_row[key]
            #             new_key = (key, left_table_name)
            #             row_dict[new_key] = value
            #
            #         left_row = row_dict
            #         left_table_rows[idx] = {
            #             "data" : left_row,
            #             "flag" : 0
            #         }
            #
            #     self.paging_state[left_table_name] = None
            #
            #     # Size checking is not conducted with assumption
            #     # 5000 rows always fit in memory
            #     self.left_data_size = asizeof.asizeof(left_table_rows)
            # 
            # else :
            #     # Handle rows in the first page
            #     total_rows = 0
            #     rows_fetched = 0
            #     page = 0
            #     for row in results:
            #         # Change the dict structure, so each column has parent table
            #         # Also, adding flag to each row
            #         if self.nodes_order == -1 or self.nodes_order == page:
            #             left_row = row
            #
            #             row_dict = {}
            #             for key in left_row:
            #                 value = left_row[key]
            #                 new_key = (key, left_table_name)
            #                 row_dict[new_key] = value
            #
            #             left_row = row_dict
            #
            #             # Save as list. 0 or 1 is a flag to identify whether a particular row has matched or has not.
            #             left_table_rows.append({
            #                 "data" : left_row,
            #                 "flag" : 0
            #             })
            #             total_rows += 1
            #         rows_fetched += 1
            #
            #         if (rows_fetched == fetch_size):
            #             self.paging_state[left_table_name] = results.paging_state
            #             page += 1
            #             break
            #     
            #     self.left_data_size = asizeof.asizeof(left_table_rows)
            #
            #     # Handle the rest of the rows
            #     while (results.has_more_pages and (self.nodes_order == -1 or self.nodes_order > page - 1 or self.nodes_order == (self.workers_count - 1))):
            #         print(f"Fetching next page of left table. Current rows: {total_rows}. Left data size: {self.left_data_size}")
            #         rows_fetched = 0
            #         ps = self.paging_state[left_table_name]
            #
            #         # Fetch based on last paging state
            #         statement = SimpleStatement(left_table_query, fetch_size=fetch_size)
            #         results = session.execute(statement, paging_state = ps)
            #
            #         for row in results:
            #             # Change the dict structure, so each column has parent table
            #             # Also, adding flag to each row
            #             if self.nodes_order == -1 or self.nodes_order == page:
            #                 left_row = row
            #
            #                 row_dict = {}
            #                 for key in left_row:
            #                     value = left_row[key]
            #                     new_key = (key, left_table_name)
            #                     row_dict[new_key] = value
            #
            #                 left_row = row_dict
            #
            #                 # Save as list. 0 or 1 is a flag to identify whether a particular row has matched or has not.
            #                 left_table_rows.append({
            #                     "data" : left_row,
            #                     "flag" : 0
            #                 })
            #                 total_rows += 1
            #                 self.left_data_size += asizeof.asizeof(row)
            #
            #             rows_fetched += 1
            #
            #             if (page != self.workers_count - 1 or self.nodes_order == -1) and (rows_fetched == fetch_size):
            #                 self.paging_state[left_table_name] = results.paging_state
            #                 page += 1
            #                 break
            #         
            #         
            #         # SIZE Checking: Checking done per page
            #         if ((self.max_data_size / 2 <= self.get_left_size()) or is_data_in_partitions or self.force_partition):
            #             print("Left data is too big. Put into partition")
            #             is_data_in_partitions = True
            #
            #             left_last_partition_id = put_into_partition_nonhash(left_table_rows, self.join_order, self.partition_max_size, self.left_table_last_partition_id, True)
            #             # Reset left_table_rows to empty list
            #             left_table_rows = []
            #             self.left_data_size = 0
            #             
            #             # Update last partition id
            #             self.left_table_last_partition_id = left_last_partition_id
            #
            
        else : # Non first order join, left table may from partitions or cassandra
            if (self.current_result == []):
                print(f"Left table for join order {self.join_order} will be fetched from disk")
                left_last_partition_id = self.left_table_last_partition_id
                is_data_in_partitions = True

                return left_table_rows, is_data_in_partitions, left_last_partition_id
                
            
            else :
                print(f"Left table for join order {self.join_order} will be fetched from memory")
                left_table_rows = self.current_result
                self.left_data_size = asizeof.asizeof(left_table_rows)

                # Reset current result
                self.current_result = []
                self.current_result_size = 0

        print("Left table successfully fetched")

        return left_table_rows, is_data_in_partitions, left_last_partition_id


    def _get_right_data(self, right_table, right_alias, is_DSE_direct_join):
        print("Fetch right table")
        # Only get data when fit to memory. Otherwise, load in join execution function
        session = self.session
        assert(session is not None)

        right_table_rows = []
        is_data_in_partitions = False
        right_last_partition_id = -1

        right_table_name = right_table
        if (right_alias != None):
            right_table_name = right_alias
        fetch_size = self.cassandra_fetch_size
        statement_and_params = []
        if (is_DSE_direct_join):
            base_right_query = self.table_query[right_table_name].removesuffix('ALLOW FILTERING')
            pks_str = self.join_metadata.get_pk_columns_string_of_table(right_table_name)
            stmt1_str = ""
            stmt2_str = ""
            stmt3_str = ""
            idx_where = base_right_query.find("where")
            if idx_where != -1 :
                stmt1_str = base_right_query[:idx_where + len("where")] + f" token({pks_str}) > ? AND token({pks_str}) < ? AND" \
                        + base_right_query[idx_where + len("where"):] + " ALLOW FILTERING"
                stmt2_str = base_right_query[:idx_where + len("where")] + f" token({pks_str}) > ? AND" \
                        + base_right_query[idx_where + len("where"):] + " ALLOW FILTERING"
                stmt3_str = base_right_query[:idx_where + len("where")] + f" token({pks_str}) < ? AND" \
                        + base_right_query[idx_where + len("where"):] + " ALLOW FILTERING"
            else:
                stmt1_str = base_right_query + f" where token({pks_str}) > ? AND token({pks_str}) < ? ALLOW FILTERING"
                stmt2_str = base_right_query + f" where token({pks_str}) > ? ALLOW FILTERING"
                stmt3_str = base_right_query + f" where token({pks_str}) < ? ALLOW FILTERING"
            stmt1 =  session.prepare(stmt1_str)
            stmt1.fetch_size = fetch_size
            stmt2 =  session.prepare(stmt2_str)
            stmt2.fetch_size = fetch_size
            stmt3 =  session.prepare(stmt3_str)
            stmt3.fetch_size = fetch_size
            for token in self.token_ranges:
                condition = token.toCondition(pks_str)
                if condition.is_always_and():
                    params = (int(condition.lhs.rhs), int(condition.rhs.rhs))
                    statement_and_params.append((stmt1, params))
                else:
                    params1 = (int(condition.lhs.rhs), )
                    params2 = (int(condition.rhs.rhs), )
                    statement_and_params.append((stmt2, params1))
                    statement_and_params.append((stmt3, params2))
            
        else:
            # left_table_queries.append(self.table_query[left_table_name])
            statement_and_params.append((self.table_query[right_table_name], None))
                
        protocol_version = session.cluster.protocol_version
        concurrency = 500 if protocol_version > 2 else 100
        results = execute_concurrent(session, statement_and_params, concurrency=concurrency, results_generator=True, raise_on_first_error=False)
        for (success, result) in results:
            if not success:
                print(result)
            else:
                for row in result:
                    right_row = row
                    tupled_key_dict = {}
                    for key in right_row:
                        value = right_row[key]
                        new_key = (key, right_table_name)
                        tupled_key_dict[new_key] = value
                    
                    right_table_rows.append({
                        "data": tupled_key_dict,
                        "flag": 0
                    })
                    self.right_data_size += asizeof.asizeof(row)
                    if ((self.max_data_size <= self.get_data_size()) or is_data_in_partitions or self.force_partition): 
                        print("Right data too big. Put into partition")
                        is_data_in_partitions = True

                        right_last_partition_id = put_into_partition_nonhash(right_table_rows, self.join_order, self.partition_max_size, self.right_table_last_partition_id, False)
                        # Reset left_table_rows to empty list
                        right_table_rows = []
                        self.right_data_size = 0
                        
                        # Update last partition id
                        self.right_table_last_partition_id = right_last_partition_id

        # right_table_query = self.table_query[right_table_name]
        # print("Right table query : ", right_table_query)
        #
        # right_table_rows = []
        # statement = SimpleStatement(right_table_query, fetch_size=self.cassandra_fetch_size)
        # results = session.execute(statement)
        #
        # if (not results.has_more_pages):
        #     right_table_rows = list(results)
        #     for idx in range(len(right_table_rows)):
        #         # Change dict structure and add flag
        #         right_row = right_table_rows[idx]
        #         row_dict = {}
        #
        #         for key in right_row:
        #             value = right_row[key]
        #             new_key = (key, right_table_name)
        #             row_dict[new_key] = value
        #
        #         right_row = row_dict
        #         right_table_rows[idx] = {
        #             "data" : right_row,
        #             "flag" : 0
        #         }
        #
        #     self.paging_state[right_table_name] = None
        #
        #     # Size checking is not conducted with assumption
        #     # 5000 rows always fit in memory
        #     self.right_data_size = asizeof.asizeof(right_table_rows)
        #
        # else :
        #     # Handle rows in first page
        #     total_rows = 0
        #     rows_fetched = 0
        #     for row in results:
        #         # Change dict structure and add flag
        #         right_row = row
        #         row_dict = {}
        #
        #         for key in right_row:
        #             value = right_row[key]
        #             new_key = (key, right_table_name)
        #             row_dict[new_key] = value
        #
        #         right_row = row_dict
        #         right_table_rows.append({
        #             "data" : right_row,
        #             "flag" : 0
        #         })
        #         rows_fetched += 1
        #         total_rows += 1
        #
        #         if (rows_fetched == self.cassandra_fetch_size):
        #             self.paging_state[right_table_name] = results.paging_state
        #             break
        #
        #         self.right_data_size += asizeof.asizeof(row)
        #
        #     # Handle the rest of the rows
        #     while (results.has_more_pages):
        #         print(f"Fetching next page of right table. Current rows: {total_rows}. Object size: {asizeof.asizeof(right_table_rows)}")
        #         rows_fetched = 0
        #         ps = self.paging_state[right_table_name]
        #
        #         # Fetch based on last paging state
        #         statement = SimpleStatement(right_table_query, fetch_size=self.cassandra_fetch_size)
        #         results = session.execute(statement, paging_state = ps)
        #
        #         for row in results:
        #             # Change the dict structure, so each column has parent table
        #             # Also, adding flag to each row
        #             right_row = row
        #
        #             row_dict = {}
        #             for key in right_row:
        #                 value = right_row[key]
        #                 new_key = (key, right_table_name)
        #                 row_dict[new_key] = value
        #
        #             right_row = row_dict
        #
        #             # Save as list. 0 or 1 is a flag to identify whether a particular row has matched or has not.
        #             right_table_rows.append({
        #                 "data" : right_row,
        #                 "flag" : 0
        #             })
        #
        #             rows_fetched += 1
        #             total_rows += 1
        #             if (rows_fetched == self.cassandra_fetch_size):
        #                 self.paging_state[right_table_name] = results.paging_state
        #                 break
        #
        #             self.right_data_size += asizeof.asizeof(row)
        #         
        #         # SIZE Checking: Check size used per page
        #         if ((self.max_data_size / 2 <= self.get_right_size()) or is_data_in_partitions or self.force_partition):
        #             print("Right data too big. Put into partition")
        #             is_data_in_partitions = True
        #
        #             right_last_partition_id = put_into_partition_nonhash(right_table_rows, self.join_order, self.partition_max_size, self.right_table_last_partition_id, False)
        #             # Reset left_table_rows to empty list
        #             right_table_rows = []
        #             self.right_data_size = 0
        #             
        #             # Update last partition id
        #             self.right_table_last_partition_id = right_last_partition_id
        #
        print("Right table successfully fetched")

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
        
        join_column_right = join_info['join_column_right']
        is_DSE_direct_join = True
        
        right_table_pks = self.join_metadata.get_pk_columns_of_table(right_table_name)
        if not isinstance(join_column_right, tuple):
            is_DSE_direct_join = len(right_table_pks) == 1 and right_table_pks[0] == join_column_right
        else:
            is_DSE_direct_join = set(right_table_pks).issubset(set(join_column_right))
        
        is_DSE_direct_join = is_DSE_direct_join and not self.disable_direct_join and len(self.token_ranges) and join_info["join_operator"] == "="

        initial_fetch_time = time.time()

        # Left table
        left_table_rows, is_left_in_partitions, left_last_partition_id = self._get_left_data(left_table, left_alias)

        # Right table
        right_table_rows, is_right_in_partitions, right_last_partition_id = self._get_right_data(right_table, right_alias, is_DSE_direct_join)

        final_fetch_time = time.time()
        fetch_time = final_fetch_time - initial_fetch_time

        key_name = "data_fetch"
        if (key_name in self.time_elapsed):
            self.time_elapsed[key_name] += fetch_time
        else :
            self.time_elapsed[key_name] = fetch_time

        print(f"Left in partition : {is_left_in_partitions}")
        print(f"Right in partition : {is_right_in_partitions}")


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
            self.partition_curr_size = 0

        return


    def _execute_both_partition(self, join_info):
        rows_executed = 0
        for left_partition_id in range(self.left_table_last_partition_id+1):
            print("Change left partition")
            left_table_rows = read_from_partition_nonhash(self.join_order, left_partition_id, True)
            should_update_left_partition = False

            
            for right_partition_id in range(self.right_table_last_partition_id+1):
                print("Change right partition")
                right_table_rows = read_from_partition_nonhash(self.join_order, right_partition_id, False)
                should_update_right_partition = False

                for left_row in left_table_rows:
                    left_row_data = left_row["data"]
                    for right_row in right_table_rows:
                        right_row_data = right_row["data"]
                        # Do join based on join type and join condition
                        merged_row = self._merge_row(join_info, left_row_data, right_row_data)

                        rows_executed += 1
                        if (rows_executed % 100000 == 0):
                            print(f"{rows_executed} rows have been executed")

                        if (merged_row == None):
                            continue

                        if (not should_update_left_partition):
                            should_update_left_partition = True
                        if (not should_update_right_partition):
                            should_update_right_partition = True

                        # Current result
                        join_order = join_info['join_order']
                        self._result_handler(merged_row, join_order)

                        # Update the flag for both left row and right row
                        left_row["flag"] = 1
                        right_row["flag"] = 1


                # Update right partition in local
                if (should_update_right_partition):
                    update_partition_nonhash(right_table_rows, self.join_order, right_partition_id, False)


            # Update left partition in local
            if (should_update_left_partition):
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
        rows_executed = 0
        for left_row in left_table_rows:
            left_row_data = left_row["data"]
            for right_row in right_table_rows:
                right_row_data = right_row["data"]
                # Do join based on join type and join condition
                merged_row = self._merge_row(join_info, left_row_data, right_row_data)

                if (merged_row == None):
                    continue

                rows_executed += 1
                if (rows_executed % 100000 == 0):
                    print(f"{rows_executed} rows have been executed")

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
        rows_executed = 0
        for left_row in left_table_rows:
            left_row_data = left_row["data"]

            for right_partition_id in range(self.right_table_last_partition_id+1):
                right_table_rows = read_from_partition_nonhash(self.join_order, right_partition_id, False)
                should_update_right_partition = False

                for right_row in right_table_rows:
                    right_row_data = right_row["data"]
                    # Do join based on join type and join condition
                    merged_row = self._merge_row(join_info, left_row_data, right_row_data)

                    rows_executed += 1
                    if (rows_executed % 100000 == 0):
                        print(f"{rows_executed} rows have been executed")

                    if (merged_row == None):
                        continue
                
                    if (not should_update_right_partition):
                        should_update_right_partition = True

                    # Current result
                    join_order = join_info['join_order']
                    self._result_handler(merged_row, join_order)

                    # Update the flag
                    left_row["flag"] = 1
                    right_row["flag"] = 1


                # Update right partition in local
                if (should_update_right_partition):
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
        rows_executed = 0
        for left_partition_id in range(self.left_table_last_partition_id+1):
            left_table_rows = read_from_partition_nonhash(self.join_order, left_partition_id, True)
            should_update_left_partition = False

            for left_row in left_table_rows:
                left_row_data = left_row["data"]
                for right_row in right_table_rows:
                    right_row_data = right_row["data"]
                    # Do join based on join type and join condition
                    merged_row = self._merge_row(join_info, left_row_data, right_row_data)


                    rows_executed += 1
                    if (rows_executed % 100000 == 0):
                        print(f"{rows_executed} rows have been executed")

                    if (merged_row == None):
                        continue

                    if (not should_update_left_partition):
                        should_update_left_partition = True

                    # Current result
                    join_order = join_info['join_order']
                    self._result_handler(merged_row, join_order)

                    # Update the flag
                    left_row["flag"] = 1
                    right_row["flag"] = 1
                    
        
            # Update left partition in local
            if (should_update_left_partition):
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
        left_join_column_data = get_value_from_dict(join_column, left_table_name, left_data)
        right_join_column_data = get_value_from_dict(join_column_right, right_table_name, right_data)

        try:
            if not isinstance(left_join_column_data, tuple):
                left_join_column_data = float(left_join_column_data)
            else:
                left_join_column_data_array = list(left_join_column_data)
                for i in range(len(left_join_column_data_array)):
                    left_join_column_data_array[i] = float(left_join_column_data_array[i])
                left_join_column_data = tuple(left_join_column_data_array)
            if not isinstance(right_join_column_data, tuple):
                right_join_column_data = float(right_join_column_data)
            else:
                right_join_column_data_array = list(right_join_column_data)
                for i in range(len(right_join_column_data_array)):
                    right_join_column_data_array[i] = float(right_join_column_data_array[i])
                right_join_column_data = tuple(right_join_column_data_array)
        
        except :
            pass

        if (operator == "="):
            if (left_join_column_data == right_join_column_data):
                should_do_join = True

        elif (operator == "<"):
            if not isinstance(left_join_column_data, tuple) and \
                    not isinstance(right_join_column_data, tuple) and \
                    (left_join_column_data < right_join_column_data):
                should_do_join = True

        elif (operator == ">"):
            if not isinstance(left_join_column_data, tuple) and \
                    not isinstance(right_join_column_data, tuple) and \
                    (left_join_column_data > right_join_column_data):
                should_do_join = True

        elif (operator == "<="):
            if not isinstance(left_join_column_data, tuple) and \
                    not isinstance(right_join_column_data, tuple) and \
                    (left_join_column_data <= right_join_column_data):
                should_do_join = True

        elif (operator == ">="):
            if not isinstance(left_join_column_data, tuple) and \
                    not isinstance(right_join_column_data, tuple) and \
                    (left_join_column_data >= right_join_column_data):
                should_do_join = True

        elif (operator == "!="):
            if not isinstance(left_join_column_data, tuple) and \
                    not isinstance(right_join_column_data, tuple) and \
                    (left_join_column_data != right_join_column_data):
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
            if (self.get_data_size() >= self.max_data_size):
                result_last_partition_id = put_into_partition_nonhash(self.current_result, next_join_order, self.partition_max_size, self.result_last_partition_id, True)

                # Update on result partition id and empty the current result
                self.result_last_partition_id = result_last_partition_id
                self.current_result = []
                self.current_result_size = 0

        else : # Some result is in partition
            # Add to self.partition then flush when it is big enough
            self.partition_curr_size += asizeof.asizeof(merged_row)
            self.partition.append(merged_row)

            if (self.partition_curr_size >= self.partition_max_size):
                result_last_partition_id = put_into_partition_nonhash(self.partition, next_join_order, self.partition_max_size, self.result_last_partition_id, True)
                self.result_last_partition_id = result_last_partition_id

                # Reset
                self.partition = []
                self.partition_curr_size = 0

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
    
    def __save_result(self, filename):
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
            final_res_path = os.path.join(tmp_path, str(final_join_order))

            for id in range(0,left_last_partition_id+1):
                partition_name = f"{id}_l.txt"
                partition_path = os.path.join(final_res_path, partition_name)
                partition_file = open(partition_path, mode='r')
                partition_lines = partition_file.readlines()

                for line in partition_lines:
                    data = json.loads(line)
                    unsatisfied = False
                    for condition in self.filter_conditions:
                        condition.set_rows(data)
                        if not condition:
                            unsatisfied = True
                            break
                    if unsatisfied:
                        continue
                    f_res.write(line)

                partition_file.close()

        
        else : # Final result is in memory (self.current_result), return immediately
            
            self.current_result = jsonTupleKeyEncoder(self.current_result)
            from decimal import Decimal
            import uuid
            from datetime import datetime
            def custom_serializer(obj):
                if isinstance(obj, Decimal):
                    return str(obj)
                if isinstance(obj, uuid.UUID):
                    return str(obj)
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

            for row in self.current_result:
                unsatisfied = False
                for condition in self.filter_conditions:
                    condition.set_rows(row)
                    if not condition:
                        unsatisfied = True
                        break
                if unsatisfied:
                    continue
                f_res.write(json.dumps(row, default=custom_serializer)+"\n")

        f_res.close()

        # Delete result
        if (not self.save_partition_trace):
            delete_prev_result(str(self.join_order))

        return self



