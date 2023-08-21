from __future__ import annotations
from ..base import Put, Put
from ..exceptions import StopProcessing, SkipRow
__all__ = ('Sink',)

class Sink:
    """
    The base Sink class. Does nothing with put values, other than output the collected dict via `process`.

    Generally you won't use this directly, only as a base for implementing other sinks.
    """
    def __init__(self) -> None:
        self._puts = {}
        self._null_puts = []
    
    def put(self, destination: str):
        """
        Put a pipeline with the given destination
        """
        put = Put()
        self._puts[destination] = put
        return put

    def put_nowhere(self):
        """
        Put a pipeline with no destination (e.g. to force a value to be calculated even if it isn't being used in the final output)
        """
        put = Put()
        self._null_puts.append(put)
        return put

    def process(self, source):
        counter = 0
        with source:
            while True:
                counter += 1
                # Reset all piplines for the next iteration
                for put in self._puts.values():
                    put.idempotent_next(counter)
                for put in self._null_puts:
                    put.idempotent_next(counter)
                if not source.valid():
                    break
                try:
                    for put in self._null_puts:
                        put.get()
                    yield {key: put.get() for key, put in self._puts.items()}
                except SkipRow:
                    continue
                except StopProcessing:
                    break
    
    def process_all(self, source, return_results=False, *args, **kwargs):
        if return_results:
            return list(self.process(source, *args, **kwargs))
        else:
            for _ in self.process(source, *args, **kwargs):
                pass