from __future__ import annotations
from ..pipeline import Put
__all__ = ('Sink',)

class Sink(Put):
    """
    The base Sink class. Does nothing with put values, other than output the collected dicts via `get`.

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


    def idempotent_next(self, idempotency_counter):
        """
        Call `idempotent_next` on all Puts in this sink
        """
        for put in self._null_puts:
            put.idempotent_next(idempotency_counter)
        for put in self._puts.values():
            put.idempotent_next(idempotency_counter)
        if self._prev is not None:
            self._prev.idempotent_next(idempotency_counter)
    
    def keys(self):
        """
        Get a list of all keys put in this sink.
        """
        keys = set(self._puts.keys())
        if self._prev is not None:
            try:
                keys.update(self._prev.keys())
            except NotImplementedError:
                raise RuntimeError('Keys for this Sink are indeterminate')
        return keys

    def get(self):
        """
        Get the current processed row value
        """
        for put in self._null_puts:
            put.guarded_get()
        whole_put = self._prev.guarded_get() if self._prev is not None else None
        put_values = {key: put.guarded_get() for key, put in self._puts.items()}
        if self._prev is None:
            return put_values
        if not put_values:
            return whole_put
        if isinstance(whole_put, dict):
            return {**whole_put, **put_values}
        raise TypeError('Sink received a non-dict value directly, and also received multiple puts.')

    def open(self):
        for put in self._puts.values():
            if not put.is_open:
                put.open()
        for put in self._null_puts:
            if not put.is_open:
                put.open()
        super().open()

    def close(self):
        for put in self._puts.values():
            if put.is_open:
                put.close()
        for put in self._null_puts:
            if put.is_open:
                put.close()
        super().close()