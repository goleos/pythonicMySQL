from pythonicMySQL import Column, MySQLType
from typing import List


class ScriptGenerator:

    script_imports = "from pythonicMySQL import MySQLObject, Table, Column"

    def __init__(self, database: str, table: str, object_type: str, columns: List[Column]):
        self.database = database
        self.table = table
        self.object_type = object_type
        self.columns = columns

    def object_class(self) -> str:
        arguments = ", ".join([f"{column.name}: {column.mysql_type.python_type.__name__}" for column in self.columns])
        variables = "\n".join([f"        self.{column.name} = {column.name}" for column in self.columns])
        columns_const = [f"        Column('{column.name}', '{column.mysql_type}', default={column.default}, " \
                         f"unsigned={column.unsigned}, primary={column.primary}, not_null={column.not_null}, " \
                         f"unique={column.unique})," for column in self.columns]
        column_lines = "\n".join(columns_const)
        script = f"""
class {self.object_type.capitalize()}:

    COLUMNS = [
{column_lines}
    ]
    
    def __init__(self, {arguments}):
        super().__init__()
{variables}
        """
        return script

    def table_class(self) -> str:
        class_name = self.table.capitalize()
        script = f"""
class {class_name}:
    def __init__(self):
        super({class_name}, self).__init__(mysql_connection=conn, database="{self.database}", table="{self.table}", 
                                            object_type={self.object_type})

    def get(self) -> List[{self.object_type}]:
        return super({class_name}, self).get()
"""
        return script

    def generate(self) -> str:
        script = f"""{ScriptGenerator.script_imports}

{self.object_class()}
{self.table_class()}
"""
        print(script)
        return script


if __name__ == "__main__":

    gen = ScriptGenerator("HelloW", "music", "song", columns=[
            Column("title", MySQLType.varchar()),
            Column("artist", MySQLType.varchar()),
            Column("album", MySQLType.varchar()),
            Column("datetime_played_UTC", MySQLType.datetime(), unique=True),
            Column("deezer_id", MySQLType.varchar()),
            Column("explicit", MySQLType.bool()),
            Column("duration", MySQLType.timedelta())
    ])

    gen.generate()