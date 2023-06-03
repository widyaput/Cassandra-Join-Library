import psutil
import time
from pympler import asizeof

from cassandra_joinlib.commands import *
from cassandra_joinlib.intermediate_result import IntermediateDirectHashResult, IntermediatePartitionedHashResult
from cassandra_joinlib.utils import *
from cassandra_joinlib.math_utils import *

from cassandra.query import dict_factory, SimpleStatement
from cassandra.concurrent import execute_concurrent
from cassandra_joinlib.join_executor import JoinExecutor

from datetime import datetime

class HashJoinExecutor(JoinExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Saving partition ID for current join
        self.current_join_partition_ids = set()

        # Override force partition
        self.force_partition = False

        # To force save partition trace
        self.save_partition_trace = False

        # FOR TESTING USAGE
        # self.max_data_size = 0
        self.filter_conditions: List[Condition] = []


    def execute(self, save_as=f"{datetime.now()}"):
        # Inherited abstract method
        # Consume the commands queue, execute join
        if self.amqp_url:
            super().execute(save_as=save_as)
        if self.session is None:
            return
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
                                                    self.table_query[table_name] += " and "
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

                # nambah kolom table kiri di metadata
                if (not is_lefttable_in_metadata):
                    left_meta_rows = session.execute(check_leftcols_query)
                    self.join_metadata.add_table(left_table)

                    for row in left_meta_rows:
                        column_name = row['column_name']
                        self.join_metadata.add_one_column(left_table, column_name)
                        if row['kind'] == 'partition_key':
                            self.join_metadata.add_pk_column(left_table, column_name)
        
                # bikin query
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
                        # kenapa malah ambil semua kolom???
                        # update: karena columns = None => asumsi ambil semua kolom
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


        print(f"Max data size: {self.max_data_size}")
        # Execute all joins based on self.joins_info
        initial_join_time = time.time()
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

    def _get_left_data(self, left_table, join_column, left_alias):
        print("Fetching left table")
        session = self.session
        assert(session is not None)

        left_table_rows = []
        is_data_in_partitions = False
        partition_ids = set()

        left_table_name = left_table
        if (left_alias != None):
            left_table_name = left_alias

        # Read data
        if (self.join_order == 1): # First join, all data are in Cassandra
            left_table_queries = []
            fetch_size = self.cassandra_fetch_size
            statement_and_params = []
            if len(self.token_ranges):
                base_left_query = self.table_query[left_table_name].removesuffix('ALLOW FILTERING')
                if "AND" in base_left_query:
                    base_left_query = base_left_query + " AND "
                else:
                    base_left_query = base_left_query + " WHERE "
                pks_str = self.join_metadata.get_pk_columns_string_of_table(left_table_name)
                stmt1 =  session.prepare(base_left_query + f"token({pks_str}) > ? AND token({pks_str}) < ?")
                stmt1.fetch_size = fetch_size
                stmt2 =  session.prepare(base_left_query + f"token({pks_str}) > ?")
                stmt2.fetch_size = fetch_size
                stmt3 =  session.prepare(base_left_query + f"token({pks_str}) < ?")
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
                for query in left_table_queries:
                    query += " ALLOW FILTERING"
                
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
                        
                        left_table_rows.append(tupled_key_dict)
                        self.left_data_size += asizeof.asizeof(row)
                        if ((self.max_data_size <= self.get_data_size()) or is_data_in_partitions or self.force_partition): 
                            print("Data to big. Put into partition")
                            # Left table is bigger than max data size in memory, use partition
                            is_data_in_partitions = True

                            # Flush left table
                            partition_ids_this_iter = put_into_partition(left_table_rows, self.join_order, left_table_name, join_column, True)
                            partition_ids = partition_ids.union(partition_ids_this_iter)

                            left_table_rows = []
                            self.left_data_size = 0
                    

            # for left_table_query in left_table_queries:
            #     # if self.nodes_order != -1:
            #     #     fetch_size = self.leftest_table_size // self.workers_count
            #     statement = SimpleStatement(left_table_query, fetch_size=fetch_size)
            #     results = session.execute(statement)
            #
            #     # print(f"Total rows: {len(list(results))}")
            #     if (not results.has_more_pages):
            #         current_rows = list(results)
            #         for idx in range(len(current_rows)):
            #             left_row = current_rows[idx]
            #
            #             tupled_key_dict = {}
            #             for key in left_row:
            #                 value = left_row[key]
            #                 new_key = (key, left_table_name)
            #                 tupled_key_dict[new_key] = value
            #             
            #             current_rows[idx] = tupled_key_dict
            #         left_table_rows = left_table_rows + current_rows
            #
            #         self.paging_state[left_table_name] = None
            #
            #         # Size checking is not conducted with assumption
            #         # 5000 rows always fit in memory
            #         self.left_data_size = asizeof.asizeof(left_table_rows)
            #     
            #
            #     else :
            #         # Handle rows in the first page
            #         total_rows = 0
            #         rows_fetched = 0
            #         # page = 0
            #         for row in results:
            #             # if self.nodes_order == -1 or self.nodes_order == page:
            #             #     left_row = row
            #             #
            #             #     tupled_key_dict = {}
            #             #     for key in left_row:
            #             #         value = left_row[key]
            #             #         new_key = (key, left_table_name)
            #             #         tupled_key_dict[new_key] = value
            #             #     
            #             #     left_table_rows.append(tupled_key_dict)
            #             #     total_rows += 1
            #             # rows_fetched += 1
            #             left_row = row
            #
            #             tupled_key_dict = {}
            #             for key in left_row:
            #                 value = left_row[key]
            #                 new_key = (key, left_table_name)
            #                 tupled_key_dict[new_key] = value
            #             
            #             left_table_rows.append(tupled_key_dict)
            #             rows_fetched += 1
            #             total_rows += 1
            #             if (rows_fetched == fetch_size):
            #                 self.paging_state[left_table_name] = results.paging_state
            #                 # page += 1
            #                 break
            #         
            #         self.left_data_size = asizeof.asizeof(left_table_rows)
            #
            #         # Handle the rest of the rows
            #         # while (results.has_more_pages and (self.nodes_order == -1 or self.nodes_order > page - 1 or self.nodes_order == (self.workers_count - 1))):
            #         while(results.has_more_pages):
            #             print(f"Fetching next page of left table. Current rows: {total_rows}. Left data size: {self.left_data_size}")
            #             rows_fetched = 0
            #             ps = self.paging_state[left_table_name]
            #
            #             # Fetch based on last paging state
            #             statement = SimpleStatement(left_table_query, fetch_size=fetch_size)
            #             results = session.execute(statement, paging_state=ps)
            #
            #             for row in results:
            #                 # print("Handling another page of cql result")
            #                 left_row = row
            #
            #                 tupled_key_dict = {}
            #                 for key in left_row:
            #                     value = left_row[key]
            #                     new_key = (key, left_table_name)
            #                     tupled_key_dict[new_key] = value
            #                 
            #                 left_table_rows.append(tupled_key_dict)
            #                 rows_fetched += 1
            #                 total_rows += 1
            #
            #                 if (rows_fetched == self.cassandra_fetch_size):
            #                     self.paging_state[left_table_name] = results.paging_state
            #                     break
            #                 
            #                 self.left_data_size += asizeof.asizeof(row)
            #                 # if self.nodes_order == -1 or self.nodes_order == page or page == self.workers_count:
            #                 #     left_row = row
            #                 #
            #                 #     tupled_key_dict = {}
            #                 #     for key in left_row:
            #                 #         value = left_row[key]
            #                 #         new_key = (key, left_table_name)
            #                 #         tupled_key_dict[new_key] = value
            #                 #     
            #                 #     left_table_rows.append(tupled_key_dict)
            #                 #     total_rows += 1
            #                 #     self.left_data_size += asizeof.asizeof(row)
            #                 # rows_fetched += 1
            #                 #
            #                 # if (page != self.workers_count or self.nodes_order == -1) and (rows_fetched == fetch_size):
            #                 #     self.paging_state[left_table_name] = results.paging_state
            #                 #     page += 1
            #                 #     break
            #                 # if (page == self.workers_count) and (rows_fetched == fetch_size + self.leftest_table_size % fetch_size):
            #                 #     self.paging_state[left_table_name] = results.paging_state
            #                 #     page += 1
            #                 #     break
            #             # Size Checking: Checking done per page
            #             if ((self.max_data_size <= self.get_data_size()) or is_data_in_partitions or self.force_partition): 
            #                 print("Data to big. Put into partition")
            #                 # Left table is bigger than max data size in memory, use partition
            #                 is_data_in_partitions = True
            #
            #                 # Flush left table
            #                 partition_ids_this_iter = put_into_partition(left_table_rows, self.join_order, left_table_name, join_column, True)
            #                 partition_ids = partition_ids.union(partition_ids_this_iter)
            #
            #                 left_table_rows = []
            #                 self.left_data_size = 0


        else : # Non first join, left table may in self.current_result or in partitions
            if (self.current_result == []): # Result of previous join is in local disk as partitions
                print(f"Left table for join order {self.join_order} will be fetched from disk")
                left_table_rows = None
                is_data_in_partitions = True
                partition_ids = self.current_join_partition_ids

                print("Left table partition ids : ", self.current_join_partition_ids)

                return left_table_rows, is_data_in_partitions, partition_ids

            else :
                print(f"Left table for join order {self.join_order} will be fetched from memory")
                left_table_rows = self.current_result
                self.left_data_size = asizeof.asizeof(left_table_rows)
                
                # Reset current result to preserve memory
                self.current_result = []

        print("Left table successfully fetched")

        return left_table_rows, is_data_in_partitions, partition_ids


    def _get_right_data(self, right_table, join_column_right, right_alias, is_left_table_partitioned, is_DSE_direct_join):
        print("Fetching right data")
        session = self.session
        assert(session is not None)

        right_table_rows = []
        is_data_in_partitions = False
        partition_ids = set()

        right_table_name = right_table

        if (right_alias != None):
            right_table_name = right_alias

        # right_table_query = self.table_query[right_table_name]
        # print("Right-Table query : ", right_table_query)
        right_table_queries = []
        fetch_size = self.cassandra_fetch_size
        statement_and_params = []
        if (is_DSE_direct_join):
            base_right_query = self.table_query[right_table_name].removesuffix('ALLOW FILTERING')
            if "AND" in base_right_query:
                base_right_query = base_right_query + " AND "
            else:
                base_right_query = base_right_query + " WHERE "
            pks_str = self.join_metadata.get_pk_columns_string_of_table(right_table_name)
            stmt1 =  session.prepare(base_right_query + f"token({pks_str}) > ? AND token({pks_str}) < ?")
            stmt1.fetch_size = fetch_size
            stmt2 =  session.prepare(base_right_query + f"token({pks_str}) > ?")
            stmt2.fetch_size = fetch_size
            stmt3 =  session.prepare(base_right_query + f"token({pks_str}) < ?")
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
            for query in right_table_queries:
                query += " ALLOW FILTERING"
            
        else:
            # left_table_queries.append(self.table_query[left_table_name])
            statement_and_params.append((self.table_query[right_table_name], None))
                
        # print("Left-Table query : ", left_table_queries)
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
                    
                    right_table_rows.append(tupled_key_dict)
                    self.left_data_size += asizeof.asizeof(row)
                    if ((self.max_data_size <= self.get_data_size()) or is_left_table_partitioned or is_data_in_partitions or self.force_partition): 
                        print("Data to big. Put into partition")
                        # Left table is bigger than max data size in memory, use partition
                        is_data_in_partitions = True

                        # Flush left table
                        partition_ids_this_iter = put_into_partition(right_table_rows, self.join_order, right_table_name, join_column_right, True)
                        partition_ids = partition_ids.union(partition_ids_this_iter)

                        left_table_rows = []
                        self.left_data_size = 0

        # right_table_rows = []
        # statement = SimpleStatement(right_table_query, fetch_size=self.cassandra_fetch_size)
        # results = session.execute(statement)
        #
        # if (not results.has_more_pages):
        #     right_table_rows = list(results)
        #     # Change dict structure to a tupled-key dict
        #     for idx in range(len(right_table_rows)):
        #         right_row = right_table_rows[idx]
        #
        #         tupled_key_dict = {}
        #         for key in right_row:
        #             value = right_row[key]
        #             new_key = (key, right_table_name)
        #             tupled_key_dict[new_key] = value
        #         
        #         right_table_rows[idx] = tupled_key_dict
        #     
        #     self.paging_state[right_table_name] = None
        #     # Size checking is not conducted with assumption:
        #     # 5000 rows always fit in memory
        #     self.right_data_size = asizeof.asizeof(right_table_rows)
        # 
        # else :
        #     # Handle rows in first page
        #     # Change dict structure
        #     total_rows = 0
        #     rows_fetched = 0
        #     for row in results:
        #         right_row = row
        #
        #         tupled_key_dict = {}
        #         for key in right_row:
        #             value = right_row[key]
        #             new_key = (key, right_table_name)
        #             tupled_key_dict[new_key] = value
        #         
        #         right_table_rows.append(tupled_key_dict)
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
        #         results = session.execute(statement, paging_state=ps)
        #
        #         for row in results:
        #             # Change the dict structure to a tupled-key dict
        #             right_row = row
        #
        #             tupled_key_dict = {}
        #             for key in right_row:
        #                 value = right_row[key]
        #                 new_key = (key, right_table_name)
        #                 tupled_key_dict[new_key] = value
        #             
        #             right_table_rows.append(tupled_key_dict)
        #             rows_fetched += 1
        #             total_rows += 1
        #
        #             if (rows_fetched == self.cassandra_fetch_size):
        #                 self.paging_state[right_table_name] = results.paging_state
        #                 break
        #
        #             self.right_data_size += asizeof.asizeof(row)
        #         
        #
        #         # SIZE Checking: Check size used per page
        #         if (((self.max_data_size <= self.get_data_size()) or (is_left_table_partitioned)) or is_data_in_partitions or self.force_partition): 
        #             print("Data to big. Put into partition")
        #             # Left table is bigger than max data size in memory, use partition
        #             is_data_in_partitions = True
        #             
        #             # Flush right table
        #             partition_ids_curr_iter = put_into_partition(right_table_rows, self.join_order, right_table_name, join_column_right, False)
        #             partition_ids = partition_ids.union(partition_ids_curr_iter)
        #             right_table_rows = []
        #             self.right_data_size = 0
        
        # Outer check size, when left in partitions, right should be in partitions as well
        if (((self.max_data_size <= self.get_data_size()) or (is_left_table_partitioned)) or is_data_in_partitions or self.force_partition): 
            is_data_in_partitions = True
            
            # Flush right table
            partition_ids_curr_iter = put_into_partition(right_table_rows, self.join_order, right_table_name, join_column_right, False)
            partition_ids = partition_ids.union(partition_ids_curr_iter)
            right_table_rows = []
            self.right_data_size = 0

        print("Right table successfully fetched")

        return right_table_rows, is_data_in_partitions, partition_ids


    #TODO: read this
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
            
        is_DSE_direct_join = True
        right_table_pks = self.join_metadata.get_pk_columns_of_table(right_table_name)
        if not isinstance(join_column_right, tuple):
            is_DSE_direct_join = len(right_table_pks) == 1 and right_table_pks[0] == join_column_right
        else:
            is_DSE_direct_join = set(right_table_pks).issubset(set(join_column_right))
        
        is_DSE_direct_join = is_DSE_direct_join and not self.disable_direct_join and len(self.token_ranges)

        initial_fetch_time = time.time()
        # Get Left table data
        left_table_rows, is_left_table_in_partitions, left_partition_ids = self._get_left_data(left_table, join_column, left_alias)

        # Get right table data
        right_table_rows, is_right_table_in_partitions, right_partition_ids = self._get_right_data(right_table, join_column_right, right_alias, is_left_table_in_partitions, is_DSE_direct_join)

        final_fetch_time = time.time()
        fetch_time = final_fetch_time - initial_fetch_time

        key_name = "data_fetch"
        if (key_name in self.time_elapsed):
            self.time_elapsed[key_name] += fetch_time
        else :
            self.time_elapsed[key_name] = fetch_time

        # print('halo sampai sini')
        # Do join based on whether partitions is used or not
        if (is_left_table_in_partitions and is_right_table_in_partitions):
            all_partition_ids = left_partition_ids.union(right_partition_ids)

            print("Left partition ids : ", left_partition_ids)
            print("Right partition ids : ", right_partition_ids)
            print("Merged Partition ids : ", all_partition_ids)

            self._execute_partition_join(join_info, next_join_info, all_partition_ids)

        else :
            max_join_res_size = asizeof.asizeof(left_table_rows) * asizeof.asizeof(right_table_rows)
            if (max_join_res_size + self.get_data_size() >= self.max_data_size):
                if (not is_left_table_in_partitions):
                    left_partition_ids = put_into_partition(left_table_rows, self.join_order, left_table_name, join_column, True)
                if (not is_right_table_in_partitions):
                    right_partition_ids = put_into_partition(right_table_rows, self.join_order, right_table_name, join_column_right, False)

                all_partition_ids = left_partition_ids.union(right_partition_ids)

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
                if not isinstance(join_column, tuple):
                    dict_key = (join_column, left_table_name)
                    left_key = row[dict_key]
                else:
                    left_key_array = []
                    for column in join_column:
                        left_key_array.append(row[(column, left_table_name)])
                    left_key = tuple(left_key_array)

                # Left table is build table by default
                is_build_table = True

                if (left_key == None):
                    if (join_type == "INNER"):
                        continue
                    elif (join_type == "LEFT_OUTER"):
                        intermediate_result.add_row_to_left_nomatch(row)
                    elif (join_type == "RIGHT_OUTER"):
                        continue
                    else :
                        intermediate_result.add_row_to_left_nomatch(row)
                
                # Use all rows to build hash table
                intermediate_result.add_row_to_intermediate(row, is_build_table)
                left_table_rows[idx] = None

            # Right table insertion to intermediate_result and clear right table rows for each iteration
            # Probe table
            for idx in range(len(right_table_rows)):
                row = right_table_rows[idx]
                if not isinstance(join_column_right, tuple):
                    dict_key = (join_column_right, right_table_name)
                    right_key = row[dict_key]
                else:
                    right_key_array = []
                    for column in join_column_right:
                        right_key_array.append(row[(column, right_table_name)])
                    right_key = tuple(right_key_array)

                # Right table is probe table by default
                is_build_table = False

                if (right_key == None):
                    if (join_type == "INNER"):
                        continue
                    elif (join_type == "LEFT_OUTER"):
                        continue
                    elif (join_type == "RIGHT_OUTER"):
                        intermediate_result.add_row_to_right_nomatch(row)
                    else:
                        intermediate_result.add_row_to_right_nomatch(row)

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
                if not isinstance(join_column_right, tuple):
                    dict_key = (join_column_right, right_table_name)
                    right_key = row[dict_key]
                else:
                    right_key_array = []
                    for column in join_column_right:
                        right_key_array.append(row[(column, right_table_name)])
                    right_key = tuple(right_key_array)

                if (right_key == None):
                    if (join_type == "INNER"):
                        continue
                    elif (join_type == "LEFT_OUTER"):
                        continue
                    elif (join_type == "RIGHT_OUTER"):
                        intermediate_result.add_row_to_right_nomatch(row)
                    else:
                        intermediate_result.add_row_to_right_nomatch(row)

                # Now that the tables have been swap, right table is now a Build table
                is_build_table = True

                intermediate_result.add_row_to_intermediate(row, is_build_table)
                right_table_rows[idx] = None

            # Left table insertion to intermediate_result and clear left table rows for each iteration
            # Probe table
            for idx in range(len(left_table_rows)):
                row = left_table_rows[idx]
                if not isinstance(join_column, tuple):
                    dict_key = (join_column, left_table_name)
                    left_key = row[dict_key]
                else:
                    left_key_array = []
                    for column in join_column:
                        left_key_array.append(row[(column, left_table_name)])
                    left_key = tuple(left_key_array)

                # Left table is build table by default
                is_build_table = False

                if (left_key == None):
                    if (join_type == "INNER"):
                        continue
                    elif (join_type == "LEFT_OUTER"):
                        intermediate_result.add_row_to_left_nomatch(row)
                    elif (join_type == "RIGHT_OUTER"):
                        continue
                    else :
                        intermediate_result.add_row_to_left_nomatch(row)

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
            partition_ids = self.current_join_partition_ids
            final_join_order = self.join_order

            tmp_path = os.path.join(cwd, 'tmpfolder')
            final_res_path = os.path.join(tmp_path, str(final_join_order))

            for id in partition_ids:
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

            # TODO: Delete all tmpfiles after join operation

        
        else : # Final result is in memory (self.current_result), return immediately

            self.current_result = jsonTupleKeyHashEncoder(self.current_result)

            for row in self.current_result:
                unsatisfied = False
                for condition in self.filter_conditions:
                    condition.set_rows(row)
                    if not condition:
                        unsatisfied = True
                        break
                if unsatisfied:
                    continue
                f_res.write(json.dumps(row)+"\n")

        f_res.close()
        # Delete result
        if (not self.save_partition_trace):
            delete_prev_result(str(self.join_order))

        return self
