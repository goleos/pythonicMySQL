from typing import Optional, Union


class Query:

    @classmethod
    def select(cls,
               database: str, table: str,
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
        return query

    @classmethod
    def insert(cls, database, table, dictionary: dict, update_if_duplicate_key: bool = False, **kwargs):
        insertion = {**dictionary, **kwargs}
        set_string = ', \n'.join([f"`{key}` = %s" for key in insertion.keys()])
        query_string = f"INSERT INTO `{database}`.`{table}` SET {set_string} "
        if not update_if_duplicate_key:
            return Query(query_string, tuple(insertion.values()))
        else:
            query_string += f" ON DUPLICATE KEY UPDATE {set_string} "
            return Query(query_string, tuple(insertion.values()) + tuple(insertion.values()))

    @classmethod
    def update(cls, database, table, mysql_id: int, dictionary: dict, **kwargs):
        update_dict = {**dictionary, **kwargs}
        set_string = ', \n'.join([f"`{key}` = %s" for key in update_dict.keys()])
        where_string = f" WHERE `id` = {str(mysql_id)} "
        query_string = f"UPDATE `{database}`.`{table}` SET {set_string} " + where_string
        return Query(query_string, tuple(update_dict.values()))

    @classmethod
    def set_autoincrement(cls, database, table, value: int = 1):


    def __init__(self, string: str, insertions: Optional[tuple] = None):
        self.insertions = insertions
        self.string = string

    def __repr__(self):
        string = self.string
        if isinstance(self.insertions, list):
            for item in self.insertions:
                string.replace("%s", str(item), 1)
        return string
