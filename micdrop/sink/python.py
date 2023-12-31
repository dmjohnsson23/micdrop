from .base import Sink
from types import SimpleNamespace
from typing import Callable
from collections import namedtuple

__all__ = ('DictsSink', 'ObjectsSink', 'ObjectsSinkSetattr', 'NamedTuplesSink', 'CallSink')

class DictsSink(Sink):
    """
    A sink that will return the results as python dicts.
    
    Good for testing, or for converting between two internal representations of the same data.
    """
    pass

class ObjectsSink(Sink):
    """
    A sink that will return the results as python objects.
    
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
    A sink that will return the results as of python objects.
    
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

class NamedTuplesSink(Sink):
    """
    A sink that will return the results as namedtuple objects.
    
    Good for testing, or for converting between two internal representations of the same data.
    """

    def __init__(self, typename='SinkOutput'):
        self.type = namedtuple(typename, self.keys())
    
    def get(self):
        return self.type(**super().get())

class CallSink(Sink):
    """
    A sink that will call a given function with the values of each iteration
    
    Good for testing, or for converting between two internal representations of the same data,
    populating dataclasses, or creating simple custom sinks.
    """
    def __init__(self, function:Callable):
        """
        :param func: The function, class, or callable object to which to pass all of the values each
            iteration. The result of the call will be returned from the sink.
        """
        super().__init__()
        self.function = function

    def get(self):
        return self.function(**super().get())