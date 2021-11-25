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
            query = Query(query_string, list(where.values()))
        else:
            query = Query(query_string)
        return query

    @classmethod
    def insert(cls, database, table, dictionary: dict, **kwargs):
        insertion = {**dictionary, **kwargs}
        set_string = ', \n'.join([f"`{key}` = %s" for key in insertion.keys()])
        query_string = f"INSERT INTO `{database}`.`{table}` SET {set_string} "
        return Query(query_string, list(insertion.values()))

    @classmethod
    def insert_on_duplicate_key_update(cls, database, table, dictionary: dict, **kwargs):
        insertion = {**dictionary, **kwargs}
        insert_part = cls.insert(database, table, dictionary, **kwargs)
        set_string = ', \n'.join([f"`{key}` = %s" for key in insertion.keys()])
        query_string = f" ON DUPLICATE KEY UPDATE {set_string} "
        return Query(insert_part.string+query_string, list(insert_part.insertions) + list(insertion.values()))

    @classmethod
    def update(cls, database, table, mysql_id: int, dictionary: dict, **kwargs):
        update_dict = {**dictionary, **kwargs}
        set_string = ', \n'.join([f"`{key}` = %s" for key in update_dict.keys()])
        where_string = f" WHERE `id` = {str(mysql_id)} "
        query_string = f"UPDATE `{database}`.`{table}` SET {set_string} " + where_string
        return Query(query_string, list(update_dict.values()))

    def __init__(self, string: str, insertions: Optional[list] = None):
        self._insertions = insertions
        self.string = string

    @property
    def insertions(self):
        if self._insertions is not None:
            return tuple(self._insertions)
        else:
            return None

    def __repr__(self):
        return self.string
