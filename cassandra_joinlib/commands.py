from typing import Any, List, Union, Sequence


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


class Condition():
    def __init__(self, lhs: Any, operator: str, rhs: Any):
        self.lhs = lhs
        self.operator = operator
        self.rhs = rhs
        self.rows = None
        found = False
        # only primary key
        if self.operator == ">":
            found = True
        if self.operator == "<":
            found = True
        if self.operator == ">=":
            found = True
        if self.operator == "<=":
            found = True
        if self.operator == "=":
            found = True
        if self.operator == "IN":
            found = True
            assert(isinstance(self.rhs, List) or isinstance(self.rhs, Sequence))
        if self.operator == 'NOT':
            found = True
        if self.operator == 'AND':
            found = True
        if self.operator == 'OR':
            found = True
        if not found:
            raise Exception("Operator not supported")

    def is_base(self) -> bool:
        return self.operator != 'NOT' and self.operator != 'AND' and self.operator != 'OR'

    def __and__(self, other):
        return Condition(self, 'AND', other)

    def __or__(self, other):
        return Condition(self, 'OR', other)

    def __invert__(self):
        return Condition(self, 'NOT', None)
    
    def __bool__(self):
        if not isinstance(self.lhs, Condition) or not isinstance(self.rhs, Condition):
            if self.rows is None:
                raise Exception("Need to call evaluate first!")
            raw_lhs = None
            raw_rhs = None
            if not isinstance(self.lhs, Condition):
                raw_lhs = self.lhs
                if (isinstance(self.lhs, str)):
                    table_name, column = self.lhs.split('.')
                    if column in self.rows:
                        raw_lhs = self.rows[column][table_name]
            if not isinstance(self.rhs, Condition):
                raw_rhs = self.rhs
                if (isinstance(self.rhs, str)):
                    table_name, column = self.rhs.split('.')
                    if column in self.rows:
                        raw_rhs = self.rows[column][table_name]
            assert(raw_lhs is not None)
            assert(raw_rhs is not None)
            if self.operator == ">":
                return raw_lhs > raw_rhs
            if self.operator == "<":
                return raw_lhs < raw_rhs
            if self.operator == ">=":
                return raw_lhs >= raw_rhs
            if self.operator == "<=":
                return raw_lhs <= raw_rhs
            if self.operator == "=":
                return raw_lhs == raw_rhs
            if self.operator == "IN":
                assert(isinstance(raw_rhs, List) or isinstance(raw_rhs, Sequence))
                return raw_lhs in raw_rhs
            raise Exception("Operator not supported")
            
        if self.operator == 'NOT':
            return not bool(self.lhs)
        if self.operator == 'AND':
            return all(map(bool, [self.lhs, self.rhs]))
        if self.operator == 'OR':
            return any(map(bool, [self.lhs, self.rhs]))

    def __str__(self):
        if self.rhs:
            return f"{self.lhs} {self.operator} {self.rhs}"
        return f"{self.operator} {self.lhs}"


class FilterExpression():
    def __init__(self, info: TableInfo, column: str, operator: str, rhs: Any) -> None:
        self.table = info
        self.column = column
        self.operator = operator
        self.rhs = rhs


class Operators():
    def __init__(self):
        pass

class FilterOperators(Operators):
    def __init__(self, expressions: Union[List[Union[FilterExpression, Operators]], Operators, FilterExpression]):
        self.expressions = expressions


class And(FilterOperators):
    def __init__(self, expressions: List[Union[FilterExpression, Operators]]):
        super().__init__(expressions)

class Or(FilterOperators):
    def __init__(self, expressions: List[Union[FilterExpression, Operators]]):
        super().__init__(expressions)

class Not(FilterOperators):
    def __init__(self, expressions: Union[FilterExpression, Operators]):
        super().__init__(expressions)


class FilterCommand(Command):
    def __init__(self, expressions: Union[FilterExpression, FilterOperators]):
        super().__init__()
        self.expressions = expressions

class FilterCommands(Command):
    def __init__(self, expressions: Condition):
        super().__init__()
        self.expressions = expressions
