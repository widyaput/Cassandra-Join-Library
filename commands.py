class Command:
    def __init__(self):
        super().__init__()
        self.type = "NOTYPE"

class JoinCommand(Command):
    def __init__(self, join_type, leftTableInfo, rightTableInfo, operator = "="):
        super().__init__()
        self.type = "JOIN"
        self.join_type = join_type
        self.left_table = leftTableInfo.table_name
        self.join_column = leftTableInfo.join_column
        self.left_alias = leftTableInfo.alias

        self.right_table = rightTableInfo.table_name
        self.join_column_right = rightTableInfo.join_column
        self.right_alias = rightTableInfo.alias

        # Set default, join is equi-join
        self.join_operator = operator

    def set_operator(self, operator):
        self.join_operator = operator

class SelectCommand(Command):
    def __init__(self, table, columns):
        super().__init__()
        self.type = "SELECT"
        self.table = table
        self.columns = columns


class ExecuteCommand(Command):
    def __init__(self):
        super().__init__()
        self.type = "EXECUTE"


# Will be used by user directly
class TableInfo():
    def __init__(self, table_name, join_column, alias = None):
        super().__init__()
        self.table_name = table_name
        self.join_column = join_column
        self.alias = alias