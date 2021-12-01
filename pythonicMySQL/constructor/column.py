from pythonicMySQL.datatypes.mysqltypes import MySQLType, INT
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Column:

    name: str
    mysql_type: MySQLType
    default: type = None
    unsigned: bool = False
    primary: bool = False
    null: bool = False
    unique: bool = False
    auto_increment: bool = False

    @property
    def description(self) -> str:
        query_str = f"`{self.name}` {self.mysql_type.description}"
        if self.unsigned:
            query_str += " unsigned"
        if not self.null:
            query_str += " NOT NULL"
        if self.default is not None:
            query_str += f" Default {self.default}"
        if self.auto_increment:
            query_str += " AUTO_INCREMENT"
        if self.unique:
            query_str += " UNIQUE"
        if self.primary:
            query_str += " PRIMARY KEY"
        return query_str
