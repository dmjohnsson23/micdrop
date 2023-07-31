from .base import PipelineSource, PipelineSink, PipelineItem
__all__ = ('Put', 'CollectDict', 'CollectList', 'SplitDelimited', 'JoinDelimited', 'SplitKeyValue', 'JoinKeyValue', 'JsonParse', 'JsonFormat')

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

class SplitDelimited(PipelineItem):
    """
    Split a string into a list based on some delimiter
    """
    def __init__(self, delimiter: str):
        self._delimiter = delimiter
    
    def process(self, value: str):
        if value is None:
            return []
        return value.split(self._delimiter)
    
class JoinDelimited(PipelineItem):
    """
    Join a list into a string with some delimiter
    """
    def __init__(self, delimiter):
        self._delimiter = delimiter
    
    def process(self, value):
        if value is not None:
            return self._delimiter.join([str(v) for v in value if v is not None])
    
class SplitKeyValue(PipelineItem):
    """
    Split a string into a dict based on some delimiter
    """
    def __init__(self, kv_delimiter, row_delimiter="\n"):
        self._kv_delimiter = kv_delimiter
        self._row_delimiter = row_delimiter
    
    def process(self, value: str):
        if value is None: 
            return {}
        if self._row_delimiter == "\n":
            value = value.splitlines()
        else:
            value = value.split(self._row_delimiter)
        return dict([v.split(self._kv_delimiter) for v in value if v is not None])
    
class JoinKeyValue(PipelineItem):
    """
    Join a dict into a string with some delimiter
    """
    def __init__(self, kv_delimiter, row_delimiter="\n"):
        self._kv_delimiter = kv_delimiter
        self._row_delimiter = row_delimiter
    
    def process(self, value):
        if value is not None:
            return self._row_delimiter.join([f"{k}{self._kv_delimiter}{v}" for k,v in value.items() if v is not None])

class JsonParse(PipelineItem):
    """
    Parse a JSON-encoded string
    """
    def process(self, value: str):
        if value is None:
            return
        from json import loads
        return loads(value)
    
class JsonFormat(PipelineItem):
    """
    Format a structure as JSON
    """
    def process(self, value):
        from json import dumps
        dumps(value)