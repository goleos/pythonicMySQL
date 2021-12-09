from __future__ import annotations
from typing import overload, Union, List, Optional
from dataclasses import dataclass, is_dataclass, field, fields
from pythonicMySQL.datatypes.mysqltypes import MySQLType, INT


ID_COLUMN = field(default=None, init=True, metadata={"name": "id", "mysql_type": INT(11), "unsigned": True,
                                                      "primary": True, "auto_increment": True})


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
        if self.unsigned: query_str += " unsigned"
        if not self.null: query_str += " NOT NULL"
        if self.default is not None: query_str += f" Default {self.default}"
        if self.auto_increment: query_str += " AUTO_INCREMENT"
        if self.unique:
            query_str += " UNIQUE"
        if self.primary:
            query_str += " PRIMARY KEY"
        return query_str


def column(mysql_type: Union[MySQLType, MySQLType.__class__], *flags: dict, default=None, unsigned: bool = False,
           null: bool = False, unique: bool = False):
    if isinstance(mysql_type, type):
        mysql_type = mysql_type()
    metadata = {
        "mysql_type": mysql_type,
        "default": default,
        "unsigned": unsigned,
        "null": null,
        "unique": unique
    }
    for flag in flags:
        metadata = {**metadata, **flag}
    if default is not None:
        return field(default=default, metadata=metadata)
    else:
        return field(metadata=metadata)


@overload
def columns(mysql_object: type, attribute: str) -> Optional[Column]: ...


@overload
def columns(mysql_object: type) -> List[Column]: ...


def columns(mysql_object: type, attribute: Optional[str] = None) -> Union[List[Column], Column, None]:
    if not is_dataclass(mysql_object):
        raise ValueError("MySQL objects need to be a dataclass")
    columns_ = []
    if attribute is None:
        fields_ = fields(mysql_object)
    else:
        fields_ = [item for item in fields(mysql_object) if item.name == attribute]
    for field_ in fields_:
        if "mysql_type" in dict(field_.metadata).keys():
            metadata = {**{"name": field_.name}, **field_.metadata}
            columns_.append(Column(**metadata))
    if attribute is not None and len(columns_) == 1:
        return columns_[0]
    elif attribute is not None and len(columns_) == 0:
        return None
    elif attribute is None:
        return sorted(columns_, key=lambda i: i.primary, reverse=True)
    else:
        raise KeyError