from __future__ import annotations
__all__ = ('OnFail', 'PipelineItemBase', 'PipelineItem', 'Source', 'Put', 'Take', 'TakeAttr', 'TakeIndex', 'ContinuedPut', 'Call', 'Invoke', 'InvokeMethod')
from typing import Callable
from contextlib import contextmanager
from enum import Enum
from ..exceptions import SkipRowException, StopProcessingException, PipelineProcessingError
from functools import partial
import logging
logger =logging.getLogger('micdrop')

class OnFail(Enum):
    fail = 'fail'
    stop = 'stop'
    skip = 'skip'
    ignore = 'ignore'
    log_and_skip = 'log_and_skip'
    log_and_ignore = 'log_and_ignore'

    def __call__(self, exception:Exception):
        if isinstance(exception , (SkipRowException,StopProcessingException)):
            raise exception
        if self is OnFail.log_and_ignore or self is OnFail.log_and_skip:
            logger.error(str(exception), exc_info=exception)
        if self is OnFail.skip or self is OnFail.log_and_skip:
            raise SkipRowException()
        if self is OnFail.stop:
            raise StopProcessingException()
        if self is OnFail.ignore or self is OnFail.log_and_ignore:
            return
        raise exception
    
    def to_pipeline_item(self):
        return FailGuard(self)

class PipelineItemBase:
    _is_open = False
    _reset_idempotency = None

    def next(self):
        """
        Advance to the next item in the pipeline. May raise `micdrop.exceptions.StopProcessingException`
        or `micdrop.exceptions.StopIteration` if the source is exhausted, or may allow `get` to do so 
        instead.
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
        self._reset_idempotency = idempotency_counter
        self.next()

    def check_progress(self):
        """
        Get a tuple of two numbers indicating the current progress. The first number is the number
        of completed items, and the second is the total items.

        Either number may be None if the source does not support counting progress, if processing
        hasn't started, or if the source contains an indeterminate number of items.
        """
        return (None, None)
    
    def get(self):
        """
        Get the current value for this pipeline. Should raise either `micdrop.exceptions.StopProcessingException`
        or `StopIteration` once the source is exhausted, if `next` did not already do so.
        """
        raise NotImplementedError('`get` must be overridden')

    def guarded_get(self, **kwargs):
        """
        Wrapper around the `get` method that will watch for any unexpected errors and wrap them appropriately.
        """
        try:
            return self.get(**kwargs)
        # Allow certain exception types to propagate
        except StopIteration:
            raise
        except SkipRowException:
            raise
        except StopProcessingException:
            raise
        except KeyboardInterrupt:
            raise
        except PipelineProcessingError:
            raise
        # Catch all others
        except Exception as e:
            raise PipelineProcessingError(f"Error in pipeline {self.repr_for_pipeline_error()}: {e}") from e

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
    
    def chain_repr(self):
        """
        `repr` variant that also displays the repr of previous values in the chain.
        """
        return self.__repr__()
    
    def repr_for_pipeline_error(self):
        """Internal function used for building error messages when handing exceptions"""
        return f"`{self.chain_repr()}`"


class Source(PipelineItemBase):
    """
    Generic base class for sources. Not used directly; you must subclass to use this.
    """
    _progress_total = None
    _progress_completed = None
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

    def check_progress(self):
        return self._progress_completed, self._progress_total

    def next(self):
        if self._progress_completed is None:
            # FIXME this should never be needed as it should be done by open()
            self._progress_completed = 0
        self._progress_completed += 1

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

    def take(self, key, on_not_found=OnFail.fail) -> Source:
        """
        Take a sub-value from the pipeline; used to split fields or destructure data.

        Shorthand for ``source >> Take('key')``
        """
        return self >> Take(key, on_not_found)
    
    def take_attr(self, key, on_not_found=OnFail.fail) -> Source:
        """
        Take a sub-value from the pipeline; used to split fields or destructure data.

        Shorthand for ``source >> TakeAttr('key')``
        """
        return self >> TakeAttr(key, on_not_found)
    
    def take_index(self) -> Source:
        """
        Take the index for the pipeline value.

        Shorthand for ``source >> TakeIndex()``.

        Will forward a value of `None` for reports that are not indexed.
        """
        return self >> TakeIndex()
    
    def open(self):
        super().open()
        self._progress_completed = 0
    
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
        return self._prev.guarded_get()

    def __lshift__(self, prev):
        self._prev = Source.create(prev)
        return self._prev
    
    def idempotent_next(self, idempotency_counter):
        self._prev.idempotent_next(idempotency_counter)
        super().idempotent_next(idempotency_counter)

    def check_progress(self):
        if self._prev is not None:
            return self._prev.check_progress()

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
    
    def __repr__(self):
        return f"{self.__class__.__name__}()"
    
    def chain_repr(self):
        if self._prev is not None:
            return f"{self._prev.chain_repr()} >> {self.__repr__()}"
        else:
            return self.__repr__()
    
    def repr_for_pipeline_error(self):
        if self._prev is not None:
            # prev.get has already been called once this iteration before if we reach this point, 
            # so this should be safe, but just in case...
            try:
                return f"`{self.chain_repr()}` with value `{repr(self._prev.get())}`"
            except Exception:
                pass # We are already handling an exception right now, and don't need another one...
        return super().repr_for_pipeline_error()

class PipelineItem(Put, Source):
    _value = None
    _is_cached = False

    def get(self):
        if not self._is_cached:
            self._value = self.process(self._prev.guarded_get())
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
        return self._prev.guarded_get()


class Take(PipelineItem):
    """
    Extracts a single value from the previous pipeline value using ``[]`` syntax (``__getitem__``).

    Example::

        source.take('dict_value') >> Take('subvalue') >> sink.put('value')
        source.take('list_value') >> Take(0) >> sink.put('value')
    """
    key = None

    def __init__(self, key, on_not_found=OnFail.fail):
        self.key = key
        self.on_not_found = on_not_found
    
    def get(self):
        value = self._prev.guarded_get()
        if value is None:
            return None
        try:
            return value[self.key]
        except KeyError as e:
            self.on_not_found(e)
        except IndexError as e:
            self.on_not_found(e)
    
    def __repr__(self):
        return f"{self.__class__.__name__}({repr(self.key)})"


class TakeAttr(Take):
    """
    Extracts a single value from the previous pipeline value using ``.`` syntax (``getattr``).

    Example::

        source.take('object_value') >> TakeAttr('subvalue') >> sink.put('value')
    """
    
    def get(self):
        value = self._prev.guarded_get()
        if value is None:
            return None
        try:
            return getattr(value, self.key)
        except AttributeError as e:
            self.on_not_found(e)
    
    def __repr__(self):
        return f"{self.__class__.__name__}({repr(self.key)})"


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
    Call a function with the pipeline value as the first argument.

    This class is used implicitly when you put a function or other callable inline in a pipeline.
    You only need to specify it explicitly if you also want to supply additional arguments.

    Note: this works the opposite of `functools.partial`; additional arguments are added *after* 
    the input argument rather than before. This means you can wrap a `Call` around a `partial` to
    specify arguments on either side of the main input argument.
    """
    def __init__(self, function: Callable, *additional_args, **additional_kwargs) -> None:
        self.function = function
        self.additional_args = additional_args
        self.additional_kwargs = additional_kwargs
    
    def process(self, value):
        return self.function(value, *self.additional_args, **self.additional_kwargs)
    
    def __repr__(self):
        return f"{self.__class__.__name__}({repr(self.function)})"
    

class Invoke(PipelineItem):
    """
    Call the pipeline value as a function with the given arguments.
    """
    def __init__(self, *additional_args, **additional_kwargs) -> None:
        self.additional_args = additional_args
        self.additional_kwargs = additional_kwargs
    
    def process(self, value):
        return value(*self.additional_args, **self.additional_kwargs)
    

class _InvokeMethodMeta(type):
    def __getattr__(self, name):
        # enables alternate syntax
        return partial(self, name)
    

class InvokeMethod(PipelineItem, metaclass=_InvokeMethodMeta):
    """
    Call a method on the given pipeline value.

    Example::

        source.take('string-column') >> InvokeMethod('replace', '-', '_') >> sink.put('string_col')
        # Alternate syntax:
        source.take('string-column') >> InvokeMethod.replace('-', '_') >> sink.put('string_col')
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
    
    def __repr__(self):
        return f"{self.__class__.__name__}({repr(self.method_name)})"


class FailGuard(PipelineItem):
    """
    A pipline item that will allow handling exceptions raised by previous pipeline items
    """
    def __init__(self, handler=OnFail.ignore, catch=None):
        self.handler = handler
        self.catch = catch

    def get(self):
        if self._is_cached:
            return self._value
        try:
            self._value = self._prev.get()
        except Exception as e:
            if self.catch is None or isinstance(e, self.catch):
                self._value = self.handler(e)
            else:
                raise
        self._is_cached = True
        return self._value