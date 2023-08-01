from .base import PipelineItem
__all__ = ('SplitDelimited', 'JoinDelimited', 'SplitKeyValue', 'JoinKeyValue', 'JsonParse', 'JsonFormat', 'RegexParse')

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

class RegexParse(PipelineItem):
    """
    Use regex to parse a string into component capture groups.
    """