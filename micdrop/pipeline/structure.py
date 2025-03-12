from .base import PipelineItem
__all__ = ('SplitDelimited', 'JoinDelimited', 'SplitKeyValue', 'JoinKeyValue', 'JsonParse', 'JsonFormat', 'RegexSearch', 'RegexMatch', 'RegexFullmatch')

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
    def __init__(self, delimiter:str):
        self._delimiter = delimiter
    
    def _wrap(self, wrapper, value):
        if value is None: 
            return None
        if wrapper is None:
            return value
        if isinstance(wrapper, str):
            return wrapper.format(value)
        if callable(wrapper):
            return wrapper(value)
        return value
    
    def process(self, value):
        if value:
            return self._wrap(self._wrap_whole, self._delimiter.join([str(self._wrap(self._wrap_items, v)) for v in value if v is not None]) or None)

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
    def __init__(self, kv_delimiter, row_delimiter="\n", wrap_keys=None, wrap_values=None, wrap_rows=None, wrap_whole=None):
        self._kv_delimiter = kv_delimiter
        self._row_delimiter = row_delimiter
        self._wrap_keys = wrap_keys
        self._wrap_values = wrap_values
        self._wrap_rows = wrap_rows
        self._wrap_whole = wrap_whole
    
    def _wrap(self, wrapper, value):
        if value is None:
            return None
        if wrapper is None:
            return value
        if isinstance(wrapper, str):
            return wrapper.format(value)
        if callable(wrapper):
            return wrapper(value)
        return value
    
    def _rows(self, items):
        for key, value in items:
            if value is None:
                continue
            key = self._wrap(self._wrap_keys, key)
            value = self._wrap(self._wrap_values, value)
            row = f"{key}{self._kv_delimiter}{value}"
            yield self._wrap(self._wrap_rows, row)
    
    def process(self, value):
        if value:
            return self._wrap(self._wrap_whole, self._row_delimiter.join(self._rows(value.items())) or None)


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


class _RegexParseBase(PipelineItem):
    r"""
    Use regex to parse a string into component capture groups. The output is a `re.Match` object,
    which you can `take` group values from.

    Example::

        with sink.take('things') >> RegexMatch(r"(\d+)(?P<name>\w+)") as regex:
            regex.take(0) >> sink.put('full match')
            regex.take(1) >> sink.put('digits')
            regex.take('name') >> sink.put('name')
    """
    def __init__(self, pattern, flags=0):
        from re import compile
        self.regex = compile(pattern, flags)


class RegexSearch(_RegexParseBase):
    def process(self, value):
        return self.regex.search(value)
    

class RegexMatch(_RegexParseBase):
    def process(self, value):
        return self.regex.match(value)
    

class RegexFullmatch(_RegexParseBase):
    def process(self, value):
        return self.regex.fullmatch(value)