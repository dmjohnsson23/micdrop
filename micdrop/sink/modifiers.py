from .base import Sink
from ..source import Source

class MultiSink(Sink):
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
        dummy_source = MultiSinkDummySource(source)
        sink_processes = [iter(sink.process(dummy_source)) for sink in self._sinks]
        with source:
            while True:
                if not source.next():
                    break
                yield [next(process) for process in sink_processes]

class MultiSinkDummySource(Source):
    def __init__(self, real_source):
        self._real_source = real_source
    
    def next(self):
        return True
    
    def open(self):
        return self._real_source.open()

    def close(self):
        return self._real_source.close()