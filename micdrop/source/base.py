from __future__ import annotations
from ..pipeline.base import PipelineSource
__all__ = ('Source',)

class Source:
    _current_index = None
    _current_value = None

    def take(self, key) -> PipelineSource:
        return SourceTake(self, key)
    
    def whole(self):
        """
        Take each row of the source data whole
        """
        return SourceWhole(self)

    def get(self, key):
        raise NotImplementedError('Source.get must be overridden')

    def next(self)-> bool:
        raise NotImplementedError('Source.next must be overridden')
    
    def open(self):
        pass

    def close(self):
        pass

    @property
    def current_index(self):
        return self._current_index

    @property
    def current_value(self):
        return self._current_value
    
    def __enter__(self):
        self.open()
        return self
    
    def __exit__(self, type, value, traceback):
        self.close()
    
    def __rshift__(self, other):
        return self.whole() >> other

class SourceTake(PipelineSource):
    _source: Source
    _key = None

    def __init__(self, source, key):
        self._source = source
        self._key = key
    
    def get(self):
        return self._source.get(self._key)

class SourceWhole(PipelineSource):
    _source: Source

    def __init__(self, source):
        self._source = source
    
    def get(self):
        return self._source._current_value