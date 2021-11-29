import datetime
from typing import Union, Tuple
from enum import Enum


class MySQLType:

    def __init__(self, name: str, length: Union[str, int] = None, enum_values: Tuple[str] = None,
                 python_type: type = None):
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
        self._python_type = python_type

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
        if self._python_type is None:
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
                return Enum
            elif self.name == "json":
                return dict
            else:
                raise KeyError
        else:
            return self.python_type

    def convert_to_python(self, mysql_value) -> object:
        return mysql_value

    def convert_to_mysql(self, python_value) -> object:
        return python_value

    def __eq__(self, other):
        return other.name == self.name

    def __repr__(self):
        return self.description


# MySQL DataTypes
def INT(digits: int = 5) -> MySQLType:
    return MySQLType("int", digits)


def DATETIME() -> MySQLType:
    return MySQLType("datetime")


def TIME() -> MySQLType:
    return MySQLType("time")


def DATE() -> MySQLType:
    return MySQLType("date")


def FLOAT(display_length: int = 10, number_of_decimals: int = 2) -> MySQLType:
    return MySQLType("float", length=f"{str(display_length)},{str(number_of_decimals)}")


def VARCHAR(characters: int = 255) -> MySQLType:
    return MySQLType("varchar", characters)


def TINYTEXT() -> MySQLType:
    return MySQLType("tinytext")


def MEDIUMTEXT() -> MySQLType:
    return MySQLType("mediumtext")


def LONGTEXT() -> MySQLType:
    return MySQLType("longtext")


def ENUM(*values: str) -> MySQLType:
    return MySQLType("enum", values)


def JSON() -> MySQLType:
    return MySQLType("json")


def DECIMAL(maximum_total_digits: int = 10, decimal_places: int = 2) -> MySQLType:
    return MySQLType("decimal", length=f"{maximum_total_digits},{decimal_places}")