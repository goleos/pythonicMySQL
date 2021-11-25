from __future__ import annotations
from pythonicMySQL.datatypes import MySQLType


class MySQLColumn:

    ID_COLUMN_NAME = "id"

    @classmethod
    def id_column(cls, name: str = None):
        if name is None:
            name = cls.ID_COLUMN_NAME
        return MySQLColumn(name, MySQLType.int(11), unsigned=True, primary=True, auto_increment=True)

    @classmethod
    def from_describe_query(cls, desc: dict) -> MySQLColumn:
        return MySQLColumn(
            name=desc['Field'],
            mysql_type=MySQLType(desc['Type'].decode()) if desc['Type'] is not None else None,
            not_null=True if desc['Null'].lower() == "no" else False,
            primary=desc['Key'] == "PRI",
            unique=desc['Key'] == "UNI",
            default=desc['Default'].decode() if desc['Default'] is not None else None,
        )

    def __init__(self, name: str, mysql_type: MySQLType, default: object = None, unsigned: bool = False,
                 primary: bool = False, not_null: bool = False, unique: bool = False, auto_increment: bool = False):
        self.unsigned = unsigned
        self.auto_increment = auto_increment
        self.unique = unique
        self.not_null = not_null
        self.primary = primary
        self.default = default
        self.mysql_type = mysql_type
        self.name = name

    @property
    def description(self) -> str:
        query_str = f"`{self.name}` {self.mysql_type.description}"
        if self.unsigned:
            query_str += " unsigned"
        if self.not_null:
            query_str += " NOT NULL"
        if self.default:
            query_str += f" Default {self.default}"
        if self.auto_increment:
            query_str += " AUTO_INCREMENT"
        if self.unique:
            query_str += " UNIQUE"
        if self.primary:
            query_str += " PRIMARY KEY"
        return query_str

    def __repr__(self):
        return self.description
