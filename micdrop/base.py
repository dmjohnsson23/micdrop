from __future__ import annotations
__all__ = ('PipelineItem', 'Source', 'PipelineSink', 'Put', 'Take', 'TakeAttr', 'Call', 'CallMethod')
from typing import Callable


class Source:
    """
    Generic base class for sources. Not used directly; you must subclass to use this.
    """
    _reset_idempotency = None
    def get(self):
        """
        Get the current value for this pipeline
        """
        raise NotImplementedError('Source.get must be overridden')
    
    def get_index(self):
        """
        Get the index of the current value for this pipeline.

        Not all sources are indexed, so in many cases this will return `None`
        """
        return None
    
    def idempotent_next(self, idempotency_counter):
        """
        Clear the current value and advance to the next row in the pipeline.

        This propagates to any downstream sources. It is safe to call multiple times so long as you
        provide the same idempotency counter each time; subsequent calls will be no-ops.
        """
        if idempotency_counter == self._reset_idempotency:
            return
        self.next()
        self._reset_idempotency = idempotency_counter

    def next(self):
        """
        Advance to the next item in the pipeline
        """
        pass
    
    def valid(self) -> bool:
        """
        Called after a call to `reset` to see if the source has a current valid value.
        
        If this ever returns `False`, it indicates that the source is exhausted; do not use
        this to indicate an empty value. Instead, return `None` from `get`.
        """
        return True

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

    def take(self, key, safe=False) -> Source:
        """
        Take a sub-value from the pipeline; used to split fields or destructure data.

        Shorthand for ``source >> Take('key')``
        """
        return self >> Take(key, safe)
    
    def take_attr(self, key, safe=False) -> Source:
        """
        Take a sub-value from the pipeline; used to split fields or destructure data.

        Shorthand for ``source >> TakeAttr('key')``
        """
        return self >> TakeAttr(key, safe)
    
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

    @classmethod
    def create(cls, item):
        if isinstance(item, Source):
            return item
        elif hasattr(item, 'to_pipeline_source'):
            return item.to_pipeline_source()
        else:
            return PipelineItem.create(item)


class PipelineSink:
    _prev: Source = None
    _reset_idempotency = None

    def get(self):
        return self._prev.get()

    def __lshift__(self, prev):
        self._prev = Source.create(prev)
        return self._prev
    
    def idempotent_next(self, idempotency_counter):
        self._prev.idempotent_next(idempotency_counter)
        if idempotency_counter == self._reset_idempotency:
            return
        self.next()
        self._reset_idempotency = idempotency_counter

    def next(self):
        """
        Advance to the next item in the pipeline
        """
        pass

    @classmethod
    def create(cls, item):
        if isinstance(item, PipelineSink):
            return item
        elif hasattr(item, 'to_pipeline_sink'):
            return item.to_pipeline_sink()
        else:
            return PipelineItem.create(item)


class PipelineItem(PipelineSink, Source):
    _value = None
    _is_cached = False

    def get(self):
        if not self._is_cached:
            self._value = self.process(self._prev.get())
            self._is_cached = True
        return self._value

    def process(self, value):
        raise NotImplementedError('PipelineItem.process must be overridden')
    
    def next(self):
        self._value = None
        self._is_cached = False
    
    @classmethod
    def create(cls, item):
        if isinstance(item, PipelineItem):
            return item
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
            raise TypeError(f"Can't use {type(item)} as a PipelineItem")

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


class TakeAttr(Take):
    """
    Extracts a single value from the previous pipeline value using ``.`` syntax (``getattr``).

    Example::

        sink.take('object_value') >> TakeAttr('subvalue') >> sink.put('value')
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