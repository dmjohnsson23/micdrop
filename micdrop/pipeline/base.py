from __future__ import annotations
__all__ = ('PipelineItem', 'PipelineSource', 'PipelineSink', 'Put', 'Take', 'TakeProperty', 'Call', 'CallMethod')
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
        elif hasattr(item, 'to_pipeline_source'):
            return item.to_pipeline_source()
        elif hasattr(item, 'to_pipeline_item'):
            return item.to_pipeline_item()
        elif isinstance(item, type) and issubclass(item, PipelineSource):
            return item()
        elif isinstance(item, dict):
            from .transformers import Lookup
            return Lookup(item)
        elif isinstance(item, str):
            return Call(item.format)
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
        elif hasattr(item, 'to_pipeline_sink'):
            return item.to_pipeline_sink()
        elif hasattr(item, 'to_pipeline_item'):
            return item.to_pipeline_item()
        elif isinstance(item, type) and issubclass(item, PipelineSink):
            return item()
        elif isinstance(item, dict):
            from .transformers import Lookup
            return Lookup(item)
        elif isinstance(item, str):
            return Call(item.format)
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
    """
    Extracts a single value from the previous pipeline value using ``[]`` syntax (``__getitem__``).

    Example::

        sink.take('dict_value') >> Take('subvalue') >> sink.put('value')
        sink.take('list_value') >> Take(0) >> sink.put('value')
    """
    key = None

    def __init__(self, key, safe=False):
        self.key = key
        self.safe = safe
    
    def get(self):
        value = self._prev.get()
        if value is None:
            return None
        try:
            return value[self.key]
        except KeyError:
            if not self.safe:
                raise


class TakeProperty(Take):
    """
    Extracts a single value from the previous pipeline value using ``.`` syntax (``getattr``).

    Example::

        sink.take('object_value') >> TakeProperty('subvalue') >> sink.put('value')
    """
    
    def get(self):
        value = self._prev.get()
        if value is None:
            return None
        try:
            return getattr(value, self.key)
        except AttributeError:
            if not self.safe:
                raise


class Call(PipelineItem):
    """
    Call a function with the pipeline value as the first argument
    """
    def __init__(self, function: Callable, *additional_args, **additional_kwargs) -> None:
        self.function = function
        self.additional_args = additional_args
        self.additional_kwargs = additional_kwargs
    
    def process(self, value):
        return self.function(value, *self.additional_args, **self.additional_kwargs)
    

class CallMethod(PipelineItem):
    """
    Call a method on the given pipeline value
    """
    def __init__(self, method_name: str, *args, **kwargs) -> None:
        self.method_name = method_name
        self.args = args
        self.kwargs = kwargs
    
    def process(self, value):
        if value is None:
            return None
        function = getattr(value, self.method_name)
        return function(*self.args, **self.kwargs)