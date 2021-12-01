from typing import Optional, List, Union, overload, Literal
from pythonicMySQL.constructor.column import Column
from pythonicMySQL.constructor import *
from pythonicMySQL.datatypes import *
from dataclasses import dataclass, field, fields


@dataclass
class MySQLObject:

    row_id: int = field(default=None, init=False, metadata={"name": "id", "mysql_type": INT(11), "unsigned": True,
                                                            "primary": True, "auto_increment": True})

    def __post_init__(self):
        for key, value in vars(self).items():
            column = columns(self, key)
            if column is not None and not isinstance(value, column.mysql_type.python_type):
                print(key)
                self.__setattr__(key, column.mysql_type.convert_to_python(value))

    def as_mysql_dict(self) -> dict:
        result = {}
        for key, value in vars(self).items():
            column = columns(self, key)
            if column is not None:
                result[column.name] = column.mysql_type.convert_to_mysql(value)
        return result


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
def columns(mysql_object: Union[MySQLObject, MySQLObject.__class__], attribute: str) -> Optional[Column]: ...


@overload
def columns(mysql_object: Union[MySQLObject, MySQLObject.__class__], attribute: Literal[None]) -> List[Column]: ...


def columns(mysql_object: Union[MySQLObject, MySQLObject.__class__],
            attribute: Optional[str] = None) -> Union[List[Column], Column, None]:
    if isinstance(mysql_object, MySQLObject):
        mysql_object = mysql_object.__class__
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
        return columns_
    else:
        raise KeyError

if __name__ == "__main__":
    @dataclass
    class Song(MySQLObject):
        name: str = column(VARCHAR)
        artist: str = column(VARCHAR)
        explicit: bool = column(BOOLEAN, default=False)


    song = Song("sd", "ss", explicit=True)
    print(song.explicit)
    print([item.name for item in columns(Song)])
    print(columns(Song, "namei"))
