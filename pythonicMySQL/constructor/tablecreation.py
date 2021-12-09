from pythonicMySQL.mysqlclient import CLIENT
from pythonicMySQL.constructor.column import Column
from typing import List


def create_table(database, table, columns: List[Column], create_database=False):
    descriptions = [column.description for column in columns]
    CLIENT.create_table(database, table, column_descriptions=descriptions, create_database=create_database)


def update_table(database, table, columns: List[Column]):
    descriptions = [column.description for column in columns]
    CLIENT.update_table_columns(database, table, column_descriptions=descriptions)