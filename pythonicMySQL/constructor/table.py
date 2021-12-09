from __future__ import annotations
from typing import Union, TypeVar, TYPE_CHECKING, overload
from pythonicMySQL.mysqlclient import CLIENT
from pythonicMySQL.constructor.column import columns
from dataclasses import is_dataclass


T = TypeVar("T")


class Table:

    def __init__(self, database: str, table: str, python_type: T):
        self._validate_dataclass(python_type)
        self.python_type = python_type
        self.database = database
        self.table = table

    @staticmethod
    def _validate_dataclass(obj):
        if not is_dataclass(obj):
            raise ValueError("Object must be a dataclass")

    def to_mysql_dict(self, obj, remove_id: bool = False) -> dict:
        self._validate_dataclass(obj)
        columns_ = columns(obj)
        mysql_dict = {}
        for key, value in obj.__dict__.items():
            matching = [column for column in columns_ if column.name == key]
            if matching:
                mysql_dict[key] = matching[0].mysql_type.convert_to_mysql(value)
        if remove_id:
            id_column = [column for column in columns_ if column.primary]
            mysql_dict.pop(id_column[0].name, '')
        return mysql_dict

    def insert(self, obj: T, update_if_exists=False, commit: bool = True):
        if update_if_exists and len([column for column in columns(obj) if column.unique]) == 0:
            raise AssertionError("You cannot use update_if_exists when there are no unique keys")
        return CLIENT.insert(self.database, self.table, self.to_mysql_dict(obj, remove_id=True),
                             update_if_duplicate_key=update_if_exists, commit=commit)

    def update(self, mysql_id: int, obj: T, commit=True):
        return CLIENT.update(self.database, self.table, mysql_id=mysql_id, dictionary=self.to_mysql_dict(obj, True),
                             commit=commit)

    def select(self, ascending: bool = True, limit: int = None, offset: int = 0, where: Union[dict, str] = " "):
        response = CLIENT.select(self.database, self.table,
                                 ascending=ascending, limit=limit, offset=offset, where=where)
        data = response.data
        result = []
        for row in data:
            insertion_object = {}
            for key, value in row.items():
                matching = [column for column in columns(self.python_type) if column.name == key]
                if matching:
                    insertion_object[key] = matching[0].mysql_type.convert_to_python(value)
            result.append(self.python_type(**insertion_object))
        return result

    @overload
    def get_item(self, _id: int):
        ...

    @overload
    def get_item(self, column: str, value: object):
        ...

    def get_item(self, *args):
        """Gets first row corresponding to the search. Raises error if there are none."""
        if isinstance(args[0], int):
            return self.select(limit=1, where={"id": args[0]})[0]
        else:
            return self.select(limit=1, where={args[0]: args[1]})[0]

    def __iter__(self):
        return iter(self.select())

    def __len__(self):
        return CLIENT.get_number_of_rows(self.database, self.table)



#

# @overload
# def table(_database: str, _table: str) -> object: ...
#
# def table(*, cls: Optional = None, mysql_connection: mysql.connector.connection.MySQLConnection = None, database: str = None,
#           table: str = None, object_type = None,
#           ascending: bool = True, limit: int = None, offset: int = 0, where: Union[dict, str] = " "):
#
#     def wrap(cls):
#
#         setattr(cls, "teuuw", 4)
#         cls.hi = lambda : print("hello")
#         return cls
#
#     return wrap
#
#
# @table
# class Songs:
#
#     def __init__(self):
#         self.h = 3
#
#
# song = Songs()

#
#
# class Table:
#
#     object_type = None
#     client = None
#
#     def __init__(self, mysql_connection: mysql.connector.connection.MySQLConnection,
#                  database: str, table: str, object_type: MySQLObject.__class__, ascending: bool = True, limit: int = None, offset: int = 0,
#                  where: Union[dict, str] = " "):
#         self.ascending = ascending
#         self.limit = limit
#         self.offset = offset
#         self.where = where
#         self.__class__.client = Client(mysql_connection)
#         self.client = self.__class__.client
#         self.__class__.object_type = object_type
#         self.table = table
#         self.database = database
#
#     def validate_columns(self):
#         mysql_description = self.client.describe_columns(self.database, self.table).fetchall()
#         assigned_columns = self.__class__.object_type.COLUMNS + Column.id_column()
#         for desc in mysql_description:
#             if desc["Field"] not in [column.name for column in assigned_columns]:
#                 raise ValueError("Columns do not match")
#
#     def create(self):
#         columns = [Column.id_column()] + self.object_type.COLUMNS
#         descriptions = [column.description for column in columns]
#         return self.client.create_table(self.database, self.table, descriptions)
#
#     def update_columns(self):
#         columns = [Column.id_column()] + self.object_type.COLUMNS
#         descriptions = [column.description for column in columns]
#         self.__class__.client.update_table_columns(self.database, self.table, descriptions)
#
#     def get(self) -> List[MySQLObject]:
#         result = self.client.select(self.database, self.table,
#                                     ascending=self.ascending, limit=self.limit, offset=self.offset, where=self.where)
#         response = SQLResponse(result)
#         data = response.data
#         result = []
#         for item in data:
#             id_ = item.pop(Column.ID_COLUMN_NAME)
#             obj = self.__class__.object_type(**item)
#             obj.mysql_row_id = id_
#             result.append(obj)
#         return result
#
#     def insert(self, obj: MySQLObject, update_if_duplicate_key: bool = True, commit=True) -> OneDMLSQLResponse:
#         cursor = self.client.insert(self.database, self.table, obj.as_mysql_dict(),
#                                     update_if_duplicate_key=update_if_duplicate_key, commit=commit)
#         return OneDMLSQLResponse(cursor)
#
#     def update(self, obj: MySQLObject, commit=True) -> OneDMLSQLResponse:
#         cursor = self.client.update(self.database, self.table, obj.mysql_row_id, obj.as_mysql_dict(), commit=commit)
#         response = OneDMLSQLResponse(cursor)
#         return response
#
#     def __getitem__(self, item: Union[tuple, int]) -> MySQLObject:
#         if isinstance(item, tuple) and len(item) == 2:
#             result = self.client.select(self.database, self.table, where={item[0]: item[1]})
#         elif isinstance(item, int):
#             result = self.client.select(self.database, self.table, where={"id": item})
#         else:
#             raise KeyError
#         if len(result) == 1:
#             return result[0]
#         else:
#             raise KeyError
#
#     def __iter__(self):
#         return iter(self.get())
