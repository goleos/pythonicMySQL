from pythonicMySQL.datatypes.mysqltypes import MySQLType
import datetime


class BOOLEAN(MySQLType):

    def __init__(self):
        super(BOOLEAN, self).__init__("tinyint", length=1, python_type=bool)

    def convert_to_mysql(self, python_value) -> object:
        return 1 if python_value is True else 0

    def convert_to_python(self, mysql_value) -> object:
        if not isinstance(mysql_value, int):
            raise ValueError
        return bool(mysql_value)


class TIMEDELTA(MySQLType):

    def __init__(self):
        super(TIMEDELTA, self).__init__("decimal", length="10,3", python_type=datetime.timedelta)

    def convert_to_python(self, mysql_value) -> datetime.timedelta:
        return datetime.timedelta(seconds=float(mysql_value))

    def convert_to_mysql(self, python_value: datetime.timedelta) -> float:
        return python_value.seconds