from .base import PipelineSource, PipelineSink, PipelineItem
__all__ = ('Put', 'CollectDict', 'CollectList', 'CollectArgsKwargs', 'CollectFormatString', 'CollectCall')

class Put(PipelineSink):
    pass

class CollectDict(PipelineSource):
    _dict: dict = None
    def __init__(self):
        self._puts = {}
    
    def get(self):
        if self._dict is None:
            self._dict = {key: put.get() for key, put in self._puts.items()}
        return self._dict
    
    def reset(self):
        self._dict = None
    
    def put(self, key):
        put = Put()
        self._puts[key] = put
        return put


class CollectList(PipelineSource):
    _list: list = None
    def __init__(self):
        self._puts = []
    
    def get(self):
        if self._list is None:
            self._list = [put.get() for put in self._puts]
        return self._list
    
    def reset(self):
        self._list = None
    
    def put(self):
        put = Put()
        self._puts.append(put)
        return put


class CollectArgsKwargs(PipelineSource):
    """
    Base class for collectors that want to allow both named and unnamed puts; do not use directly
    """
    def __init__(self):
        self._args = []
        self._kwargs = {}
    
    def get(self):
        return (
            [put.get() for put in self._args],
            {key: put.get() for key, put in self._kwargs.items()}
        )
    
    def put(self, key=None):
        put = Put()
        if key is None:
            self._args.append(put)
        else:
            self._kwargs[key] = put
        return put
    
class CollectFormatString(CollectArgsKwargs):
    """
    Collector that allows using put operations to populate fields in a format string
    """
    _value = None
    def __init__(self, format_string:str):
        super().__init__()
        self.format_string = format_string

    def get(self):
        if self._value is None:
            args, kwargs = super().get()
            self._value = self.format_string.format(*args, **kwargs)
        return self._value
    
    def reset(self):
        self._value = None


class CollectCall(CollectArgsKwargs):
    """
    Collector that allows using put operations to populate arguments to a function or callable
    """
    _value = None
    def __init__(self, func):
        super().__init__()
        self.func = func

    def get(self):
        if self._value is None:
            args, kwargs = super().get()
            self._value = self.func(*args, **kwargs)
        return self.value
    
    def reset(self):
        self._value = None
