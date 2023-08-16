from __future__ import annotations
__all__ = ('PipelineItem', 'PipelineSource', 'PipelineSink', 'Put', 'Take', 'Call')
from typing import Callable
class PipelineSource:
    def get(self):
        raise NotImplementedError('PipelineSource.get must be overridden')
    
    def reset(self):
        pass

    def __rshift__(self, next):
        next = PipelineSink.create(next)
        next._prev = self
        return next
    
    def __call__(self, func) -> PipelineItem:
        """
        Allows usage as a decorator
        """
        func = Call(func)
        func._prev = self
        return func

    def take(self, key, safe=False) -> PipelineSource:
        """
        Take a sub-value from the pipeline; used to split fields or destructure data.
        """
        return self >> Take(key, safe)
    
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    @classmethod
    def create(cls, item):
        if isinstance(item, PipelineSource):
            return item
        elif hasattr(item, 'to_pipeline_item'):
            return item.to_pipeline_item()
        elif isinstance(item, type) and issubclass(item, PipelineSource):
            return item()
        elif callable(item):
            return Call(item)
        else:
            raise TypeError(f"Can't use {type(item)} as a PipelineSource")

class PipelineSink:
    _prev: PipelineSource = None

    def get(self):
        return self._prev.get()

    def __lshift__(self, prev):
        self._prev = PipelineSource.create(prev)
        return self._prev
    
    def reset(self):
        self._prev.reset()
    
    @classmethod
    def create(cls, item):
        if isinstance(item, PipelineSink):
            return item
        elif hasattr(item, 'to_pipeline_item'):
            return item.to_pipeline_item()
        elif isinstance(item, type) and issubclass(item, PipelineSink):
            return item()
        elif callable(item):
            return Call(item)
        else:
            raise TypeError(f"Can't use {type(item)} as a PipelineSink")


class PipelineItem(PipelineSource, PipelineSink):
    _value = None
    _is_cached = False

    def get(self):
        if not self._is_cached:
            self._value = self.process(self._prev.get())
            self._is_cached = True
        return self._value

    def process(self, value):
        raise NotImplementedError('PipelineItem.process must be overridden')
    
    def reset(self):
        self._value = None
        self._is_cached = False
        self._prev.reset()

class Put(PipelineSink):
    pass

class Take(PipelineItem):
    key = None

    def __init__(self, key, safe=False):
        self.key = key
        self.safe = safe
    
    def get(self):
        value = self._prev.get()
        if value is None:
            return None
        if self.safe:
            return
        try:
            return value[self.key]
        except KeyError:
            if not self.safe:
                raise


class Call(PipelineItem):
    def __init__(self, function: Callable) -> None:
        self.function = function
    
    def process(self, value):
        return self.function(value)