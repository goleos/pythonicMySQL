import mysql.connector
from typing import List, Union
from pythonicMySQL.constructor.column import Column
from pythonicMySQL.constructor.mysqlobject import MySQLObject
from pythonicMySQL.mysqlclient.response import SQLResponse, OneDMLSQLResponse
from pythonicMySQL.mysqlclient.client import Client


class Table:

    object_type = None

    def __init__(self, mysql_connection: mysql.connector.connection.MySQLConnection,
                 database: str, table: str, object_type: MySQLObject.__class__, ascending: bool = True, limit: int = None, offset: int = 0,
                 where: Union[dict, str] = " "):
        self.ascending = ascending
        self.limit = limit
        self.offset = offset
        self.where = where
        self.client = Client(mysql_connection)
        self.__class__.object_type = object_type
        self.table = table
        self.database = database

    def validate_columns(self):
        mysql_description = self.client.describe_columns(self.database, self.table).fetchall()
        assigned_columns = self.__class__.object_type.COLUMNS + Column.id_column()
        for desc in mysql_description:
            if desc["Field"] not in [column.name for column in assigned_columns]:
                raise ValueError("Columns do not match")

    def create(self):
        columns = [Column.id_column()] + self.object_type.COLUMNS
        descriptions = [column.description for column in columns]
        return self.client.create_table(self.database, self.table, descriptions)

    def get(self) -> List[MySQLObject]:
        result = self.client.select(self.database, self.table,
                                    ascending=self.ascending, limit=self.limit, offset=self.offset, where=self.where)
        response = SQLResponse(result)
        data = response.data
        result = []
        for item in data:
            id_ = item.pop(Column.ID_COLUMN_NAME)
            obj = self.__class__.object_type(**item)
            obj.mysql_row_id = id_
            result.append(obj)
        return result

    def insert(self, obj: MySQLObject, update_if_duplicate_key: bool = True, commit=True) -> OneDMLSQLResponse:
        cursor = self.client.insert(self.database, self.table, obj.as_mysql_dict(),
                                    update_if_duplicate_key=update_if_duplicate_key, commit=commit)
        return OneDMLSQLResponse(cursor)

    def update(self, obj: MySQLObject, commit=True) -> OneDMLSQLResponse:
        cursor = self.client.update(self.database, self.table, obj.mysql_row_id, obj.as_mysql_dict(), commit=commit)
        response = OneDMLSQLResponse(cursor)
        return response

    def __getitem__(self, item: Union[tuple, int]) -> MySQLObject:
        if isinstance(item, tuple) and len(item) == 2:
            result = self.client.select(self.database, self.table, where={item[0]: item[1]})
        elif isinstance(item, int):
            result = self.client.select(self.database, self.table, where={"id": item})
        else:
            raise KeyError
        if len(result) == 1:
            return result[0]
        else:
            raise KeyError

    def __iter__(self):
        return iter(self.get())

