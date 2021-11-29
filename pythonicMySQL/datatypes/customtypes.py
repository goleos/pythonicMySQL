from pythonicMySQL.datatypes.mysqltypes import MySQLType
import datetime


class BOOL(MySQLType):

    def __init__(self):
        super(BOOL, self).__init__("tinyint", length=1)

    def convert_to_mysql(self, python_value) -> object:
        return python_value

    def convert_to_python(self, mysql_value) -> object:
        if not isinstance(mysql_value, int):
            raise ValueError
        return bool(mysql_value)


class TIMEDELTA(MySQLType):

    def __init__(self):
        super(TIMEDELTA, self).__init__("decimal", length="10,3")

    def convert_to_python(self, mysql_value) -> datetime.timedelta:
        return datetime.timedelta(seconds=float(mysql_value))

    def convert_to_mysql(self, python_value: datetime.timedelta) -> float:
        return python_value.seconds