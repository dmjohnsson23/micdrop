from __future__ import annotations
__all__ = ('PipelineItem', 'Source', 'Put', 'Take', 'TakeAttr', 'TakeIndex', 'ContinuedPut', 'Call', 'CallMethod')
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
        next = Put.create(next)
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
    
    def take_index(self) -> Source:
        """
        Take the index for the pipeline value.

        Shorthand for ``source >> TakeIndex()``.

        Will forward a value of `None` for reports that are not indexed.
        """
        return self >> TakeIndex()
    
    def open(self):
        pass

    def close(self):
        pass
    
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


class Put:
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

    @property
    def then(self) -> Source:
        """
        Continue the pipeline after this put.

        Shorthand for ``ContinuedPut(put)``.

        Example::

            source.take('value') >> sink.put('original').then >> int >> sink.put('as_int')
            # The above is equivalent to:
            with source.take('value') as value:
                value >> sink.put('original')
                value >> int >> sink.put('as_int')
        """
        return ContinuedPut(self)

    @classmethod
    def create(cls, item):
        if isinstance(item, Put):
            return item
        elif hasattr(item, 'to_pipeline_put'):
            return item.to_pipeline_put()
        else:
            return PipelineItem.create(item)

class PipelineItem(Put, Source):
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
        elif isinstance(item, type) and issubclass(item, Put):
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


class ContinuedPut(PipelineItem):
    """
    Used to place a Put in the middle of a pipeline without ending the pipeline.

    Example::

        source.take('value') >> ContinuedPut(sink.put('original')) >> int >> sink.put('as_int')
        # The above is equivalent to:
        with source.take('value') as value:
            value >> sink.put('original')
            value >> int >> sink.put('as_int')
    """
    def __init__(self, put:Put):
        self >> put
    
    def get(self):
        return self._prev.get()


class Take(PipelineItem):
    """
    Extracts a single value from the previous pipeline value using ``[]`` syntax (``__getitem__``).

    Example::

        source.take('dict_value') >> Take('subvalue') >> sink.put('value')
        source.take('list_value') >> Take(0) >> sink.put('value')
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

        source.take('object_value') >> TakeAttr('subvalue') >> sink.put('value')
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


class TakeIndex(PipelineItem):
    """
    Extracts the index value from the pipeline item

    Example::

        source >> TakeIndex() >> sink.put('id')
    """
    
    def get(self):
        return self._prev.get_index()


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