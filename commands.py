class Command:
    def __init__(self):
        super().__init__()
        self.type = "NOTYPE"

class JoinCommand(Command):
    def __init__(self, join_type, left_table, join_column, right_table, join_column_right = None):
        super().__init__()
        self.type = "JOIN"
        self.join_type = join_type
        self.left_table = left_table
        self.right_table = right_table
        self.join_column = join_column
        self.join_column_right = join_column_right

        # Set default, join is equi-join
        self.join_operator = "="

    def set_operator(self, operator):
        self.join_operator = operator

class SelectCommand(Command):
    def __init__(self, table, column_name, condition):
        super().__init__()
        self.type = "SELECT"
        self.table = table
        self.column_name = column_name
        self.condition = condition


class ExecuteCommand(Command):
    def __init__(self):
        super().__init__()
        self.type = "EXECUTE"

    
class TableInfo():
    def __init__(self, table_name, join_column, alias = None):
        super().__init__()
        self.name = table_name
        self.join_column = join_column
        self.alias = alias