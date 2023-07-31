from .base import Source
from typing import Sequence, Mapping
__all__ = ('ObjectsSource', 'DictsSource')

class DictsSource(Source):
    """
    Use an iterable of python dicts (or, technically, any object supporting __get__) as a source.

    Good for testing, or for converting between two internal representations of the same data.
    """
    def __init__(self, dicts: Sequence[Mapping]):
        self._dicts = iter(dicts)

    def get(self, key):
        return self._current_value[key]

    def open(self):
        self._current_index = 0

    def next(self):
        try:
            self._current_value = next(self._dicts)
            self._current_index += 1
            return True
        except StopIteration:
            return False

class ObjectsSource(Source):
    """
    Use an iterable of python objects as a source.

    Good for testing, or for converting between two internal representations of the same data.
    """
    def __init__(self, objects: Sequence[object]):
        self._objects = iter(objects)

    def get(self, key):
        return getattr(self._current_value, key)

    def open(self):
        self._current_index = 0
        
    def next(self):
        try:
            self._current_value = next(self._objects)
            self._current_index += 1
            return True
        except StopIteration:
            return False