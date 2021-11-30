import copy
from mysql.connector.connection import MySQLConnection
from pythonicMySQL.mysqlclient.query import Query
from typing import Union, List


class Client:

    def __init__(self, connection: MySQLConnection):
        self.connection = connection
        self.cursor = connection.cursor(dictionary=True)

    def commit(self):
        self.connection.commit()

    def execute_query(self, query: Query, commit: bool = False):
        self.cursor.execute(query.string, query.insertions)
        if commit:
            self.commit()
        return self.cursor

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
        return self.execute_query(query)

    def insert(self, database, table, dictionary: dict, update_if_duplicate_key: bool = False,
               commit: bool = True, **kwargs):
        insertion = {**dictionary, **kwargs}
        set_string = ', \n'.join([f"`{key}` = %s" for key in insertion.keys()])
        query_string = f"INSERT INTO `{database}`.`{table}` SET {set_string} "
        if not update_if_duplicate_key:
            query = Query(query_string, tuple(insertion.values()))
        else:
            query_string += f" ON DUPLICATE KEY UPDATE {set_string} "
            query = Query(query_string, tuple(insertion.values()) + tuple(insertion.values()))
        result = copy.copy(self.execute_query(query, commit))
        self.set_autoincrement(database, table, 1)
        return result

    def update(self, database, table, mysql_id: int, dictionary: dict, commit: bool = True, **kwargs):
        update_dict = {**dictionary, **kwargs}
        set_string = ', \n'.join([f"`{key}` = %s" for key in update_dict.keys()])
        where_string = f" WHERE `id` = {str(mysql_id)} "
        query_string = f"UPDATE `{database}`.`{table}` SET {set_string} " + where_string
        query = Query(query_string, tuple(update_dict.values()))
        result = copy.copy(self.execute_query(query, commit))
        self.set_autoincrement(database, table, 1)
        return result

    def set_autoincrement(self, database: str, table: str, value: int):
        query = Query(f"ALTER TABLE `{database}`.`{table}` AUTO_INCREMENT = {value}")
        return self.execute_query(query, commit=True)

    def describe_columns(self, database: str, table: str):
        query = Query(f"DESCRIBE `{database}`.`{table}`")
        return self.execute_query(query)

    def create_table(self, database: str, table: str, column_descriptions: List[str]):
        columns_expression = ", \n".join(column_descriptions)
        query = f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` ( {columns_expression})"
        return self.execute_query(Query(query), commit=True)

    def update_table_columns(self, database, table, column_descriptions: List[str]):
        columns_expression = ", \n".join(column_descriptions)
        query = f"ALTER TABLE `{database}`.`{table}` MODIFY {columns_expression}"
        return self.execute_query(Query(query), commit=True)


