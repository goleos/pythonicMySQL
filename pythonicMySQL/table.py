import mysql.connector
from typing import List, Union
from pythonicMySQL.column import Column
from pythonicMySQL.query import Query
from pythonicMySQL.mysqlobject import MySQLObject
from pythonicMySQL.sqlresponse import SQLResponse, OneDMLSQLResponse


class Table:

    mysql_connection = None
    mysql_cursor = None
    object_type = None

    def __init__(self, mysql_connection: mysql.connector.connection.MySQLConnection,
                 database: str, table: str, object_type: MySQLObject.__class__, filter_query: Query=None):
        Table.mysql_connection = mysql_connection
        Table.mysql_cursor = Table.mysql_connection.cursor(dictionary=True)
        self.__class__.object_type = object_type
        self.table = table
        self.database = database
        if filter_query is None:
            self.filter_query: Query = Query.select(database, table)
        else:
            self.filter_query = filter_query

    @classmethod
    def execute_query(cls, query: Query, commit: bool = False) -> mysql.connector.connection.MySQLCursorDict:
        cls.mysql_cursor.execute(query.string, query.insertions)
        result = cls.mysql_cursor
        if commit:
            cls.mysql_connection.commit()
        return result

    @property
    def columns_description(self) -> List[Column]:
        query = Query(f"DESCRIBE `{self.database}`.`{self.table}`")
        result = Table.execute_query(query)
        columns = []
        for column_desc in result:
            columns.append(Column.from_describe_query(column_desc))
        return columns

    @property
    def create_query(self) -> Query:
        columns = [Column.id_column()] + self.object_type.COLUMNS
        columns_expression = ", \n".join(column.description for column in columns)
        query = f"CREATE TABLE IF NOT EXISTS `{self.database}`.`{self.table}` ( {columns_expression})"
        return Query(query)

    @classmethod
    def generate_class_script(cls, database: str, table: str, object_type: type):
        script = f"""
    def __init__(self):
        super({cls.__name__}, self).__init__(mysql_connection=conn, database="{database}", table="{table}", 
                                            object_type={object_type.__name__})
        
    def get(self, query: Query=None) -> List[{object_type.__name__}]:
        return super({cls.__name__}, self).get(query)
"""
        print(script)
        return script

    def get(self, query: Query=None) -> List[MySQLObject]:
        if query is None:
            query = self.filter_query
        response = SQLResponse(Table.execute_query(query))
        data = response.data
        result = []
        for item in data:
            id_ = item.pop(Column.ID_COLUMN_NAME)
            obj = self.__class__.object_type(**item)
            obj.mysql_row_id = id_
            result.append(obj)
        return result

    def insert(self, obj: MySQLObject, commit=True) -> OneDMLSQLResponse:
        dictionary = obj.as_dict()
        query = Query.insert(self.database, self.table, dictionary)
        cursor = Table.execute_query(query, commit=commit)
        return OneDMLSQLResponse(cursor)

    def insert_or_update(self, obj: MySQLObject, commit=True) -> OneDMLSQLResponse:
        dictionary = obj.as_dict()
        query = Query.insert(self.database, self.table, dictionary, update_if_duplicate_key=True)
        cursor = Table.execute_query(query, commit=commit)
        response = OneDMLSQLResponse(cursor)
        self.set_autoincrement(1)
        return response

    def update(self, obj: MySQLObject, commit=True) -> OneDMLSQLResponse:
        query = Query.update(self.database, self.table, mysql_id=obj.mysql_row_id, dictionary=obj.as_dict())
        cursor = Table.execute_query(query, commit=commit)
        response = OneDMLSQLResponse(cursor)
        self.set_autoincrement(1)
        return response

    def set_autoincrement(self, value: int):
        query = Query(f"ALTER TABLE `{self.database}`.`{self.table}` AUTO_INCREMENT = {value}")
        Table.execute_query(query, commit=True)

    def create(self):
        Table.execute_query(self.create_query, True)

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

