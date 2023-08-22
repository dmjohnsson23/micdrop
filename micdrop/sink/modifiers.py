from .base import Sink
from ..exceptions import SkipRow, StopProcessing
__all__ = ('MultiSink',)

class MultiSink:
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

    def process(self, source):
        counter = 0
        with source.opened:
            while True:
                counter += 1
                try:
                    for sink in self._sinks:
                        sink.idempotent_next(counter)
                    yield [sink.get() for sink in self._sinks]
                except SkipRow:
                    continue
                except (StopProcessing, StopIteration):
                    break

    def process_all(self, source, return_results=False, *args, **kwargs):
        if return_results:
            return list(self.process(source, *args, **kwargs))
        else:
            for _ in self.process(source, *args, **kwargs):
                pass