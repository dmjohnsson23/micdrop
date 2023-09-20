from .base import Sink
from ..pipeline import PipelineItemBase
from ..exceptions import SkipRowException, StopProcessingException
__all__ = ('MultiSink',)

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