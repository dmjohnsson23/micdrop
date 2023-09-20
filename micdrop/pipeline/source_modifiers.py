from .base import Source
from ..exceptions import SkipRowException, StopProcessingException
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

    def idempotent_next(self, idempotency_counter):
        counter = 0
        while True:
            try:
                self.source.idempotent_next((counter, idempotency_counter))
                if self.condition(self.source.get()):
                    return
            except SkipRowException:
                continue
            except (StopIteration, StopProcessingException):
                break
    
    def open(self):
        return self.source.open()

    def close(self):
        return self.source.close()