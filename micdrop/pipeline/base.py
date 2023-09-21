from __future__ import annotations
__all__ = ('PipelineItemBase', 'PipelineItem', 'Source', 'Put', 'Take', 'TakeAttr', 'TakeIndex', 'ContinuedPut', 'Call', 'Invoke', 'InvokeMethod')
from typing import Callable
from contextlib import contextmanager

class PipelineItemBase:
    _is_open = False
    _reset_idempotency = None

    def next(self):
        """
        Advance to the next item in the pipeline. May raise `micdrop.exceptions.StopProcessingException` or
        `micdrop.exceptions.StopIteration` if the source is exhausted, or may allow `get` 
        to do so instead.
        """
        pass
    
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
    
    def get(self):
        """
        Get the current value for this pipeline. Should raise either `micdrop.exceptions.StopProcessingException`
        or `StopIteration` once the source is exhausted, if `next` did not already do so.
        """
        raise NotImplementedError('`get` must be overridden')

    def open(self):
        self._is_open = True

    def close(self):
        self._is_open = False

    @property
    def is_open(self):
        return self._is_open

    @property
    @contextmanager
    def opened(self):
        """
        Context manager to open and close the source/sink for processing.

        Note that this is different from using the source as a context manager directly. This
        method actually calls `open` and `close`, whereas the other is just for syntactic sugar.

        This is used internally by `Sink.process`, and of limited use elsewhere unless you are
        trying to implement your own processing loop.

        ::

            # Use a source as a context manager directly when building the pipeline...
            with IterableSource(dicts) as source:
                source.take('thing1') >> sink.put('thing_1')
                source.take('thing2') >> sink.put('thing_2')

            # ...but use source.opened when actually processing the source.
            def process_source(source):
                '''Yields all the rows of the source'''
                counter = 0
                with source.opened:
                    while True:
                        counter += 1
                        try:
                            source.idempotent_next(counter)
                            yield source.get()
                        except SkipRowException:
                            continue
                        except (StopProcessingException, StopIteration):
                            break
        """
        self.open()
        yield self
        self.close()


class Source(PipelineItemBase):
    """
    Generic base class for sources. Not used directly; you must subclass to use this.
    """
    def keys(self):
        """
        Get a list of all keys that can be taken with `take`.

        This returns all keys that the source supports, not necessarily those contained in any 
        particular row.

        Not all source types necessarily implement this method, as it is understood that not all 
        sources will have well-defined schemas that can be detected. It is likely only useful to
        implement on origin sources, to enable auto-mapping.
        """
        raise NotImplementedError(f'Source.keys is not supported for {self.__class__.__name__}')
    
    def get_index(self):
        """
        Get the index of the current value for this pipeline.

        Not all sources are indexed, so in many cases this will return `None`
        """
        return None

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
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        pass

    @classmethod
    def create(cls, item):
        if isinstance(item, Source):
            return item
        elif hasattr(item, 'to_pipeline_source'):
            return item.to_pipeline_source()
        else:
            return PipelineItem.create(item)


class Put(PipelineItemBase):
    _prev: Source = None

    def get(self):
        return self._prev.get()

    def __lshift__(self, prev):
        self._prev = Source.create(prev)
        return self._prev
    
    def idempotent_next(self, idempotency_counter):
        self._prev.idempotent_next(idempotency_counter)
        super().idempotent_next(idempotency_counter)

    @property
    def then(self) -> ContinuedPut:
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

    def open(self):
        super().open()
        if self._prev is not None and not self._prev.is_open:
            self._prev.open()

    def close(self):
        super().close()
        if self._prev is not None and self._prev.is_open:
            self._prev.close()

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
    
    def __rshift__(self, next):
        if self._prev is None:
            # Create a pipeline segment since this item has no source
            from .segment import PipelineSegment
            return PipelineSegment() >> self >> next
        else:
            return super().__rshift__(next)
    
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

    def open(self):
        Put.open(self)
        Source.open(self)

    def close(self):
        Source.close(self)
        Put.close(self)


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
    

class Invoke(PipelineItem):
    """
    Call the pipeline value as a function with the given arguments.
    """
    def __init__(self, *additional_args, **additional_kwargs) -> None:
        self.additional_args = additional_args
        self.additional_kwargs = additional_kwargs
    
    def process(self, value):
        return value(*self.additional_args, **self.additional_kwargs)
    

class InvokeMethod(PipelineItem):
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