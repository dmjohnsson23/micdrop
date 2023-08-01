from .base import Source
from typing import Callable
__all__ = ('FilteredSource',)

class FilteredSource(Source):
    """
    A wrapper around another source to filter certain rows.
    """
    def __init__(self, source:Source, condition:Callable):
        """
        :param source: The source to wrap
        :param condition: The function to call to check each row; will receive the row as its only parameter
        """
        self.source = source
        self.condition = condition

    def get(self, key):
        return self.source.get(key)

    def next(self)-> bool:
        while self.source.next():
            if self.condition(self.source.current_value):
                return True
        return False
    
    def open(self):
        return self.source.open()

    def close(self):
        return self.source.close()