from __future__ import annotations
from ..pipeline.base import Put
from ..exceptions import StopProcessing, SkipRow
__all__ = ('Sink',)

class Sink:
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
        with source:
            while True:
                try:
                    if not source.next():
                        break
                    for put in self._null_puts:
                        put.get()
                    yield {key: put.get() for key, put in self._puts.items()}
                except SkipRow:
                    continue
                except StopProcessing:
                    break
                finally:
                    # Reset all piplines for the next iteration
                    for put in self._puts.values():
                        put.reset()
                    for put in self._null_puts:
                        put.reset()
    
    def process_all(self, source, return_results=False, *args, **kwargs):
        if return_results:
            return list(self.process(source, *args, **kwargs))
        else:
            for _ in self.process(source, *args, **kwargs):
                pass