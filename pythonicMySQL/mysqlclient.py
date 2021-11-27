from mysql.connector.connection import MySQLConnection, MySQLCursorDict
from query import Query
from typing import Optional, Union, List


class MySQLResponse:

    def __init__(self, cursor: MySQLCursorDict):
        self._cursor = cursor
        self._data = self._cursor.fetchall()
        self._query = Query(self._cursor.statement)
        self._rowcount = self._cursor.rowcount
        self._lastrowid = self._cursor.lastrowid

    @property
    def query(self) -> Query:
        return self._query

    @property
    def data(self) -> List[dict]:
        return self._data

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def __repr__(self):
        return str({
            "rowcount": self.rowcount,
            "data": self.data,
            "query": self.query
        })


class MySQLClient:

    def __init__(self, connection: MySQLConnection):
        self.connection = connection
        self.cursor = connection.cursor(dictionary=True)

    def commit(self):
        self.connection.commit()

    def execute_query(self, query: Query, commit: bool = False):
        self.cursor.execute(query.string, query.insertions)
        if commit:
            self.commit()

    def select(self, database: str, table: str,
               ascending: bool = True, limit: int = None, offset: int = 0, where: Union[dict, str] = " "):
        select = f" SELECT * FROM `{database}`.`{table}` "
        if isinstance(where, dict):
            where_strings = [f"`{wkey}` = %s" for wkey in where.keys()]
            where_ = f"WHERE {' AND '.join(where_strings)} " if where_strings else ' '
        else:
            where_ = " " + where + " "
        order = " ORDER BY `id` ASC " if ascending else " ORDER BY `id` DESC "
        limit_ = f" LIMIT {offset}, {limit} " if limit else ""
        query_string = select + where_ + order + limit_
        if isinstance(where, dict):
            query = Query(query_string, tuple(where.values()))
        else:
            query = Query(query_string)



