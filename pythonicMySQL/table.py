import mysql.connector
from typing import List, Union
from pythonicMySQL.column import MySQLColumn
from pythonicMySQL.query import Query
from pythonicMySQL.mysqlobject import MySQLObject


class MySQLTable:

    mysql_connection = None
    mysql_cursor = None

    @classmethod
    def execute_query(cls, query: Query, commit: bool = False):
        cls.mysql_cursor.execute(query.string, query.insertions)
        result = cls.mysql_cursor
        if commit:
            cls.mysql_connection.commit()
        return result

    def __init__(self, database: str, table: str, object_type: MySQLObject.__class__, filter_query: Query=None):
        if MySQLTable.mysql_cursor is None:
            MySQLTable.mysql_cursor = MySQLTable.mysql_connection.cursor(dictionary=True)
        self.table = table
        self.database = database
        self.object_type = object_type
        if filter_query is None:
            self.filter_query: Query = Query.select(database, table)
        else:
            self.filter_query = filter_query

    @property
    def columns_description(self) -> List[MySQLColumn]:
        query = Query(f"DESCRIBE `{self.database}`.`{self.table}`")
        result = MySQLTable.execute_query(query)
        columns = []
        for column_desc in result:
            columns.append(MySQLColumn.from_describe_query(column_desc))
        return columns

    @property
    def create_query(self) -> Query:
        columns = [MySQLColumn.id_column()] + self.object_type.COLUMNS
        columns_expression = ", \n".join(column.description for column in columns)
        query = f"CREATE TABLE IF NOT EXISTS `{self.database}`.`{self.table}` ( {columns_expression})"
        return Query(query)

    def get(self, query: Query=None) -> List[MySQLObject]:
        if query is None:
            query = self.filter_query
        data = MySQLTable.execute_query(query).fetchall()
        result = []
        for item in data:
            id_ = item.pop(MySQLColumn.ID_COLUMN_NAME)
            obj = self.object_type(**item)
            obj.mysql_row_id = id_
            result.append(obj)
        return result

    def insert(self, obj: MySQLObject, commit=True):
        dictionary = obj.as_dict()
        query = Query.insert(self.database, self.table, dictionary)
        cursor = MySQLTable.execute_query(query, commit=commit)
        return cursor

    def insert_or_update(self, obj: MySQLObject, commit=True):
        dictionary = obj.as_dict()
        query = Query.insert_on_duplicate_key_update(self.database, self.table, dictionary)
        cursor = MySQLTable.execute_query(query, commit=commit)
        self.set_autoincrement(1)
        return cursor

    def update(self, obj: MySQLObject, commit=True):
        query = Query.update(self.database, self.table, mysql_id=obj.mysql_row_id, dictionary=obj.as_dict())
        cursor = MySQLTable.execute_query(query, commit=commit)
        self.set_autoincrement(1)
        return cursor

    def set_autoincrement(self, value: int):
        query = Query(f"ALTER TABLE `{self.database}`.`{self.table}` AUTO_INCREMENT = {value}")
        MySQLTable.execute_query(query, commit=True)

    def create(self):
        MySQLTable.execute_query(self.create_query, True)

    def __getitem__(self, item: Union[tuple, int]) -> MySQLObject:
        if isinstance(item, tuple) and len(item) == 2:
            query = Query.select(self.database, self.table, where={item[0]: item[1]})
            result = self.get(query)
        elif isinstance(item, int):
            query = Query.select(self.database, self.table, where={"id": item})
            result = self.get(query)
        else:
            raise KeyError
        if len(result) == 1:
            return result[0]
        else:
            raise KeyError

    def __iter__(self):
        return iter(self.get())

