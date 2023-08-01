from __future__ import annotations
__all__ = ('PipelineItem', 'PipelineSource', 'PipelineSink', 'Take', 'Call')
class PipelineSource:
    def get(self):
        raise NotImplementedError('PipelineSource.get must be overridden')
    
    def reset(self):
        pass

    def __rshift__(self, next):
        if isinstance(next, PipelineSink):
            pass # Nothing to do
        elif isinstance(next, type) and issubclass(next, PipelineSink):
            next = next()
        elif callable(next):
            next = Call(next)
        else:
            raise TypeError(f"Can't use {type(next)} in conversion pipeline")
        next._prev = self
        return next
    
    def __call__(self, func) -> PipelineItem:
        """
        Allows usage as a decorator
        """
        func = Call(func)
        func._prev = self
        return func

    def take(self, key) -> PipelineSource:
        """
        Take a sub-value from the pipeline; used to split fields or destructure data.
        """
        return self >> Take(key)
    
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass
class PipelineSink:
    _prev: PipelineSource

    def get(self):
        return self._prev.get()

    def __lshift__(self, prev):
        if isinstance(prev, PipelineSource):
            self._prev = prev
        elif isinstance(prev, type) and issubclass(prev, PipelineSource):
            self._prev = prev()
        elif callable(prev):
            self._prev = Call(prev)
        else:
            raise TypeError(f"Can't use {type(prev)} in conversion pipeline")
        return self._prev
    
    def reset(self):
        self._prev.reset()


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


class Take(PipelineItem):
    _key = None

    def __init__(self, key):
        self._key = key
    
    def get(self):
        return self._prev.get()[self._key]


class Call(PipelineItem):
    def __init__(self, function) -> None:
        self.function = function
    
    def process(self, value):
        return self.function(value)