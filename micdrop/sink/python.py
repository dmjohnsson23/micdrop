from .base import Sink
from types import SimpleNamespace
__all__ = ('DictsSink', 'ObjectsSink', 'ObjectsSinkSetattr')

class DictsSink(Sink):
    """
    A sink that will return the results as an iterable of python dicts.
    
    Good for testing, or for converting between two internal representations of the same data.
    """
    pass

class ObjectsSink(Sink):
    """
    A sink that will return the results as an iterable of python objects.
    
    Good for testing, or for converting between two internal representations of the same data.
    """
    def __init__(self, type:type=SimpleNamespace):
        """
        :param type: Optionally a type or type factory to get the objects to update. (Note that
        the object's __dict__ will be updated directly, bypassing properties)
        """
        super().__init__()
        self._type = type

    def process(self, source, *args, **kwargs):
        for data in super().process(source, *args, **kwargs):
            obj = self._type()
            obj.__dict__.update(data)
            yield obj


class ObjectsSinkSetattr(Sink):
    """
    A sink that will return the results as an iterable of python objects.
    
    Good for testing, or for converting between two internal representations of the same data.
    """
    def __init__(self, type:type=SimpleNamespace):
        """
        :param type: Optionally a type or type factory to get the objects to update. (Note that
        setattr will be called, meaning properties will be invoked)
        """
        super().__init__()
        self._type = type

    def process(self, source, *args, **kwargs):
        for data in super().process(source, *args, **kwargs):
            obj = self._type()
            for k, v in data.items():
                setattr(obj, k, v)
            yield obj