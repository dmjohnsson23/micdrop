"""
A collection of pipeline items that are "loose ends", e.g. either do not pull from a source or output to a sink
"""
__all__ = ('FactorySource', 'StaticSource', 'IterableSource')

from .base import Source

class FactorySource(Source):
    """
    A source that calls the given factory function each iteration to get a value, rather than pulling
    from the primary source. Useful to supply values that do not exist in the original data.

    Example::

        FactorySource(time_ns) >> sink.put('name')
    
    A factory source is always considered valid, but can be combined with `SentinelStop` or `SentinelSkip`
    if desired.
    """
    _value = None
    _is_cached = False

    def __init__(self, factory) -> None:
        self._factory = factory

    def get(self):
        if not self._is_cached:
            self._value = self._factory()
            self._is_cached = True
        return self._value
    
    def next(self):
        self._value = None
        self._is_cached = False


class StaticSource(Source):
    """
    A source that supplies the same value every iteration. Useful to supply values that do not exist 
    in the original data.

    Example::

        StaticSource('Other') >> sink.put('type')
    
    A static source is always considered valid and always returns the same value.
    """
    def __init__(self, value) -> None:
        self._value = value

    def get(self):
        return self._value
    

class IterableSource(Source):
    """
    A source that supplies a value from an iterable The iterable must be as long as, or longer than,  
    the number of records in the main source. Useful to supply values that do not exist in the
    original data.

    Example::

        IterableSource(range(99)) >> sink.put('id')
    """
    _value = None
    _is_cached = False
    
    def __init__(self, iterable) -> None:
        self._iterable = iter(iterable)
        self._valid = True

    def get(self):
        return self._value
            
    
    def next(self):
        self._value = None
        self._is_cached = False
        try:
            self._value = next(self._iterable)
        except StopIteration:
            self._value = None
            self._valid = False
        self._is_cached = True
    
    def valid(self):
        return self._valid