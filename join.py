from commands import *
from intermediate_result import IntermediateResult

class JoinExecutor:
    def __init__(self, session, keyspace_name, table_name):
        super().__init__()
        # These attributes are about the DB from Cassandra
        self.session = session
        self.keyspace = keyspace_name

        # Saving current result for the join process
        self.current_result = None

        # Saving commands for Lazy execution
        self.command_queue = []

        # Set a left table as the join process is a deep left-join
        self.left_table = table_name

        # Saving queries for each table needs (Select and Where Query)
        self.table_query = {}

        # Saving tempFileMeta objects as partitioning may be needed for big data.
        self.temp_files = []

        # Set default select query on left_table
        self.table_query[table_name] = f"SELECT * FROM {table_name}"

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


                # Join preparation has been prepared, now continue to execute_join
                self._execute_join(join_type, right_table, join_column)


        return self.current_result
    
    def _execute_join(self, join_type, right_table, join_column):

        session = self.session

        # Check whether current result is available
        intermediate_result = IntermediateResult(join_column)

        left_table_rows = None
        right_table_rows = None

        if (self.current_result == None): # Current Result is None
            
            # Left table is from self.left_table
            left_table_query = self.table_query[self.left_table]

            print("Left-Table query : ", left_table_query)

            left_table_rows = session.execute(left_table_query)

            # Right table
            right_table_query = self.table_query[right_table]

            print("Right-Table query : ", right_table_query)

            right_table_rows = session.execute(right_table_query)

        
        else : # Current Result is Available
            
            # Left table is from self.current_result
            left_table_rows = self.current_result

            # Right table
            right_table_query = self.table_query[right_table]

            print("Right-Table query : ", right_table_query)

            right_table_rows = session.execute(right_table_query)
        

        # Left table insertion to intermediate_result
        for row in left_table_rows:
                intermediate_result.add_row_to_intermediate(row, True)
        
        # Right table insertion to intermediate_result
        for row in right_table_rows:
                intermediate_result.add_row_to_intermediate(row, False)

        
        result = intermediate_result.build_result()

        # Append result as current result
        self.current_result = result

        return