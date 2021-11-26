import copy

from mysql.connector.cursor import MySQLCursorDict
from pythonicMySQL.query import Query
from typing import List



class SQLResponse:

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


class OneDMLSQLResponse(SQLResponse):

    @property
    def assigned_id(self) -> int:
        return self._lastrowid

    @property
    def had_effect(self) -> bool:
        return bool(self.rowcount)

    @property
    def row_created(self) -> bool:
        return True if self.rowcount == 1 else False

    def __repr__(self):
        query_string = self.query.string
        query_string.replace("\n", " ")
        return str({
            "assigned_id": self.assigned_id,
            "had_effect": self.had_effect,
            "row_created": self.row_created,
            "query": query_string
        })
