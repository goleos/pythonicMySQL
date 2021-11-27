import datetime
from typing import Union, Tuple, overload
from enum import Enum


class MySQLType:

    @classmethod
    def int(cls, digits: int = 5):
        return cls("int", digits)

    @classmethod
    def bool(cls):
        return cls("tinyint", 1)

    @classmethod
    def datetime(cls):
        return cls("datetime")

    @classmethod
    def time(cls):
        return cls("time")

    @classmethod
    def date(cls):
        return cls("date")

    @classmethod
    def float(cls, display_length: int = 10, number_of_decimals: int = 2):
        return cls("float", length=f"{str(display_length)},{str(number_of_decimals)}")

    @classmethod
    def varchar(cls, characters: int = 255):
        return cls("varchar", characters)

    @classmethod
    def tinytext(cls):
        return cls("tinytext")

    @classmethod
    def mediumtext(cls):
        return cls("mediumtext")

    @classmethod
    def longtext(cls):
        return cls("longtext")

    @classmethod
    def enum(cls, *values: str):
        return cls("enum", values)

    @classmethod
    def timedelta(cls):
        return cls("float", length="10,3", forced_type=datetime.timedelta)

    def __init__(self, name: str, length: Union[str, int] = None, enum_values: Tuple[str] = None,
                 forced_type: type = None):
        name = name.lower()
        if " " in name:
            name = name.split(" ")[0]
        if "(" in name:
            split = name.split("(")
            text = split[0]
            self.name = text
            replacement = split[1].replace(")", "")
            try:
                self.length = int(replacement)
            except ValueError:
                self.length = None
        else:
            self.name = name
            self.length = length
            self.enum_values = enum_values
        self.forced_type = forced_type

    @property
    def description(self) -> str:
        capitalised_name = self.name.upper()
        if self.length is not None:
            return f"{capitalised_name}({str(self.length)})"
        elif self.enum_values is not None:
            values = (('\"' + value + '\"') for value in self.enum_values)
            return f"{capitalised_name}({', '.join(values)})"
        else:
            return capitalised_name

    @property
    def python_type(self) -> type:
        if self.forced_type is None:
            if self.name == "tinyint" and self.length == 1:
                return bool
            elif self.name in ("tinyint", "int", "smallint", "mediumint", "bigint"):
                return int
            elif self.name in ("float", "double", "decimal"):
                return float
            elif self.name == "datetime":
                return datetime.datetime
            elif self.name == "time":
                return datetime.time
            elif self.name == "date":
                return datetime.date
            elif self.name in ("char", "varchar", "blob", "text", "tinyblob", "tinytext", "mediumblob", "mediumtext",
                               "longblob", "longtext"):
                return str
            elif self.name == "enum":
                return Union[Enum, str]
            else:
                raise KeyError
        else:
            return self.forced_type

    def to_python_value(self, mysql_value):
        if self.name == "tinyint" and self.length == 1:
            return bool(mysql_value)
        elif self.forced_type == datetime.timedelta:
            return datetime.timedelta(seconds=mysql_value)

    def to_mysql_value(self, python_value):
        if isinstance(python_value, datetime.timedelta):
            return python_value.seconds

    def __eq__(self, other):
        return other.name == self.name

    def __repr__(self):
        return self.description
