from .base import Sink
from ..pipeline.base import Put
from ..pipeline.loose import PuppetSource
from ..pipeline import PipelineItemBase
from ..exceptions import SkipRowException, StopProcessingException
__all__ = ('MultiSink', 'RepeaterSink')

class MultiSink(PipelineItemBase):
    """
    Sink wrapper that allows one source to send data to multiple sinks with only one process loop
    """
    def __init__(self, *sinks:Sink, **named_sinks:Sink):
        self._sinks = [*sinks, *named_sinks.values()]
        self._named_sinks = named_sinks
    
    def put(self, sink_index, key=None):
        if isinstance(sink_index, int):
            return self._sinks[sink_index].put(key)
        else:
            return self._named_sinks[sink_index].put(key)
    
    def __getattr__(self, name):
        return self._named_sinks[name]
    
    def idempotent_next(self, idempotency_counter):
        for sink in self._sinks:
            sink.idempotent_next(idempotency_counter)

    def get(self):
        return [sink.get() for sink in self._sinks]

    def open(self):
        for sink in self._sinks:
            if not sink.is_open:
                sink.open()
        super().open()

    def close(self):
        for sink in self._sinks:
            if sink.is_open:
                sink.close()
        super().close()


class RepeaterSink(Sink):
    """
    Sink wrapper that will pass received values to the inner sink multiple times, iterating over 
    values of a specified field and passing the others straight.

    This has a similar use case to the `Flatten` pipeline item, but Flatten is more ergonomic for 
    destructuring highly structured data, whereas RepeaterSink is more ergonomic for cases where 
    the same data needs to be inserted multiple times with only minor variations each time.

    Example::

        sink = RepeaterSink(Sink())

        source.take('id_list') >> sink.put_each('id')
        source.take('val1') >> sink.put('val1')
        source.take('val2') >> sink.put('val2')
    
    Given input data like this:

    +---------+------+------+
    | id_list | val1 | val2 |
    +=========+======+======+
    | [1,2,3] | 'a'  | 'b'  |
    +---------+------+------+
    | [4,5]   | 'c'  | 'd'  |
    +---------+------+------+
    | [6]     | 'e'  | 'f'  |
    +---------+------+------+
    
    Would result in an output like this:

    +----+------+------+
    | id | val1 | val2 |
    +====+======+======+
    | 1  | 'a'  | 'b'  |
    +----+------+------+
    | 2  | 'a'  | 'b'  |
    +----+------+------+
    | 3  | 'a'  | 'b'  |
    +----+------+------+
    | 4  | 'c'  | 'd'  |
    +----+------+------+
    | 5  | 'c'  | 'd'  |
    +----+------+------+
    | 6  | 'e'  | 'f'  |
    +----+------+------+
    """
    def __init__(self, wrapped:Sink):
        super().__init__()
        self._iter_puts = {}
        self._puppet = PuppetSource()
        self._wrapped = wrapped
        self._puppet >> self._wrapped

    def put_each(self, destination: str):
        """
        Put a pipeline with the given destination, which should receive an iterable. Each value of
        the iterable will be sent to the wrapped sink separately.
        """
        put = Put()
        self._iter_puts[destination] = put
        return put
    
    def get(self):
        static_values = super().get()
        iter_keys = tuple(self._iter_puts.keys()) # so we guarantee always iterating in the same order
        iter_values = zip(*[self._iter_puts[key].get() for key in iter_keys])
        gotten = []
        for value_set in iter_values:
            dynamic_values = dict(zip(iter_keys, value_set))
            self._puppet.set({**static_values, **dynamic_values})
            gotten.append(self._wrapped.get())
        return gotten

    def idempotent_next(self, idempotency_counter):
        super().idempotent_next(idempotency_counter)
        for put in self._iter_puts.values():
            put.idempotent_next(idempotency_counter)
