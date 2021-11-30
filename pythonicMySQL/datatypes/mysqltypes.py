from typing import Union, List
from pythonicMySQL.datatypes.typeconversion import MYSQLTYPES
from enum import EnumMeta


class MySQLType:

    def __init__(self, name: str, length: Union[str, int] = None, enum_values: List[str] = None,
                 python_type: type = None):
        name = name.upper()
        # if " " in name:
        #     name = name.split(" ")[0]
        # if "(" in name:
        #     split = name.split("(")
        #     text = split[0]
        #     self.name = text
        #     replacement = split[1].replace(")", "")
        #     try:
        #         self.length = int(replacement)
        #     except ValueError:
        #         self.length = None
        self.name = name
        self.length = length
        self.enum_values = enum_values
        self._python_type = python_type

    @property
    def description(self) -> str:
        if self.length is not None:
            return f"{self.name}({str(self.length)})"
        elif self.enum_values is not None:
            values = (('\"' + value + '\"') for value in self.enum_values)
            return f"{self.name}({', '.join(values)})"
        else:
            return self.name

    @property
    def python_type(self) -> type:
        if not self._python_type:
            return MYSQLTYPES[self.name]
        else:
            return self._python_type

    def convert_to_python(self, mysql_value) -> object:
        return mysql_value

    def convert_to_mysql(self, python_value) -> object:
        return python_value

    def __eq__(self, other):
        return other.name == self.name

    def __repr__(self):
        return self.description


# MySQL DataTypes
class INT(MySQLType):

    def __init__(self, digits: int = 5):
        super().__init__("int", digits)


class DATETIME(MySQLType):

    def __init__(self):
        super().__init__("datetime")


class TIME(MySQLType):

    def __init__(self):
        super().__init__("time")


class DATE(MySQLType):

    def __init__(self):
        super().__init__("date")


class FLOAT(MySQLType):

    def __init__(self, display_length: int = 10, number_of_decimals: int = 2):
        super().__init__("float", length=f"{str(display_length)},{str(number_of_decimals)}")


class VARCHAR(MySQLType):

    def __init__(self, characters: int = 255):
        super().__init__("varchar", characters)


class TINYTEXT(MySQLType):

    def __init__(self):
        super().__init__("tinytext")


class MEDIUMTEXT(MySQLType):

    def __init__(self):
        super().__init__("mediumtext")


class LONGTEXT(MySQLType):

    def __init__(self):
        super().__init__("longtext")


class ENUM(MySQLType):
    
    def __init__(self, enum: EnumMeta):
        self.enum = enum
        super(ENUM, self).__init__(name="enum", enum_values=[str(e.value) for e in self.enum])

    def convert_to_mysql(self, python_value: EnumMeta) -> str:
        return str(python_value.value)

    def convert_to_python(self, mysql_value: str) -> EnumMeta:
        return self.enum(mysql_value)


class JSON(MySQLType):

    def __init__(self):
        super().__init__("json")


class DECIMAL(MySQLType):

    def __init__(self, maximum_total_digits: int = 10, decimal_places: int = 2):
        super().__init__("decimal", length=f"{maximum_total_digits},{decimal_places}")
