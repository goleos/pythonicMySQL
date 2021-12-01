import datetime
import enum
from typing import Dict

MYSQLTYPES = {
    "INT": int,
    "DATETIME": datetime.datetime,
    "TIME": datetime.time,
    "DATE": datetime.date,
    "FLOAT": float,
    "VARCHAR": str,
    "TINYTEXT": str,
    "MEDIUMTEXT": str,
    "LONGTEXT": str,
    "ENUM": enum.EnumMeta,
    "JSON": str,
    "DECIMAL": float
}
