from typing import Optional


class Query:

    def __init__(self, string: str, insertions: Optional[tuple] = None):
        self.insertions = insertions
        self.string = string

    def __repr__(self):
        string = self.string
        if isinstance(self.insertions, list):
            for item in self.insertions:
                string.replace("%s", str(item), 1)
        return string
