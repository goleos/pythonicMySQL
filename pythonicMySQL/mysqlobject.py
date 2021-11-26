from typing import Optional, List
from pythonicMySQL.column import Column
from pythonicMySQL.datatypes import MySQLType


class MySQLRow:

    def __init__(self, mysql_columns: List[Column], row_id: Optional[int] = None):
        self.mysql_columns = [Column.id_column()] + mysql_columns
        self.mysql_row_id = row_id

    def column_from_attribute(self, attribute: str) -> Optional[Column]:
        if attribute[0] == "_":
            attribute = attribute[1:]
        for column in self.mysql_columns:
            if attribute == column.name:
                return column
        return None

    def as_dict(self) -> dict:
        result = {}
        for key, value in vars(self).items():
            column = self.column_from_attribute(key)
            if column is not None:
                result[column.name] = value
        return result

    @staticmethod
    def _convert_to_python_value(value, column: Column):
        result = value
        if column.mysql_type == MySQLType.bool():
            result = bool(value)
        return result

    def __setattr__(self, key, value):
        new_value = value
        try:
            restricted_column_names = [column.name for column in self.mysql_columns]
            if self.mysql_row_id is not None:
                if key == "mysql_row_id" and value is not None:
                    raise NotImplementedError
                elif key in restricted_column_names:
                    raise NotImplementedError
            new_value = self._convert_to_python_value(value, self.column_from_attribute(key))
        except AttributeError:
            pass
        super().__setattr__(key, new_value)

    def __repr__(self):
        return str(self.as_dict())


class MySQLObject(MySQLRow):

    COLUMNS: List[Column] = []

    def __init__(self, **additional_attrs):
        columns = self.__class__.COLUMNS
        super().__init__(mysql_columns=columns)
        for attr, value in additional_attrs.items():
            self.__setattr__(attr, value)

    @classmethod
    def generate_init_script(cls) -> str:
        arguments = ", ".join([f"{column.name}: {column.mysql_type.python_type.__name__}" for column in cls.COLUMNS])
        variables = "\n".join([f"    self.{column.name} = {column.name}" for column in cls.COLUMNS])
        script = f"""
def __init__(self, {arguments}):
    super().__init__()
{variables}
"""
        print(script)
        return script

