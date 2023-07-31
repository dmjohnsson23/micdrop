from __future__ import annotations
from ..pipeline.base import PipelineSink
__all__ = ('Sink',)

class Sink:
    def __init__(self) -> None:
        self._puts = {}
    
    def put(self, destination: str):
        put = SinkPut()
        self._puts[destination] = put
        return put

    def process(self, source):
        with source:
            while True:
                if not source.next():
                    break
                yield {key: put.get() for key, put in self._puts.items()}
                # Reset all piplines for the next iteration
                for put in self._puts.values():
                    put.reset()
    
    def process_all(self, source, return_results=False, *args, **kwargs):
        if return_results:
            return list(self.process(source, *args, **kwargs))
        else:
            for _ in self.process(source, *args, **kwargs):
                pass


class SinkPut(PipelineSink):
    pass