"""
A collection of pipeline items that are "loose ends", e.g. either do not pull from a source or output to a sink
"""
__all__ = ('LooseSink', 'FactorySource', 'StaticSource', 'IterableSource')

from .base import PipelineSink, PipelineSource

class LooseSink(PipelineSink):
    """
    A "loose" sink not bound to the main sink. You can use this get get transitory data that may be 
    useful during processing but which does not need to be saved in the final output.
    """
    pass

class FactorySource(PipelineSource):
    """
    A source that calls the given factory function each iteration to get a value, rather than pulling
    from the primary source. Useful to supply values that do not exist in the original data.
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
    
    def reset(self):
        self._value = None
        self._is_cached = False

class StaticSource(PipelineSource):
    """
    A source that supplies the same value every iteration. Useful to supply values that do not exist in the 
    original data.
    """
    def __init__(self, value) -> None:
        self._value = value

    def get(self):
        return self._value
    
class IterableSource(PipelineSource):
    """
    A source that supplies a value from an iterable The iterable must be as long as, or longer than, the number 
    of records in the main source. Useful to supply values that do not exist in the original data.
    """
    _value = None
    _is_cached = False
    
    def __init__(self, iterable) -> None:
        self._iterable = iter(iterable)

    def get(self):
        if not self._is_cached:
            self._value = next(self._iterable)
            self._is_cached = True
        return self._value
    
    def reset(self):
        self._value = None
        self._is_cached = False