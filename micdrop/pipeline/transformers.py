"""
Collection of pipeline items that perform various transformation
"""
__all__ = ('ParseDatetime', 'FormatDatetime', 'ParseDate', 'FormatDate', 'ParseBoolean', 'FormatBoolean', 'Lookup', 'StringReplace', 'Default')

from .base import PipelineItem
    
class ParseDatetime(PipelineItem):
    def __init__(self, format='%Y-%m-%d %H:%I:%S', zero_date=False):
        """
        Read a string as a datetime.datetime object

        :param format: The format string to use when interpreting
        :param zero_date: If "zero dates" exist that should be converted to `None`, e.g. "0000-00-00 00:00:00"
        """
        self._format = format
        self._zero_date = zero_date
    
    def process(self, value):
        from datetime import datetime
        if self._zero_date:
            zero_time = datetime(2000, 2, 2, 2, 2, 2).strftime(self._format).replace('2', '0')
            if value == zero_time:
                return None
        if value is not None:
            return datetime.strptime(value, self._format)
    
class FormatDatetime(PipelineItem):
    def __init__(self, format='%Y-%m-%d %H:%I:%S', zero_date=False):
        """
        Format a datetime.datetime object as a string

        :param format: The format string to use when formatting
        :param zero_date: If `None` should be converted into a "zero date", e.g. "0000-00-00 00:00:00"
        """
        self._format = format
        self._zero_date = zero_date
    
    def process(self, value):
        from datetime import datetime
        if self._zero_date:
            zero_time = datetime(2000, 2, 2, 2, 2, 2).strftime(self._format).replace('2', '0')
            if value is None:
                return zero_time
        if value is not None:
            return value.strftime(self._format)
    
class ParseDate(PipelineItem):
    def __init__(self, format='%Y-%m-%d', zero_date=False):
        """
        Read a string as a datetime.date object

        :param format: The format string to use when interpreting
        :param zero_date: If "zero dates" exist that should be converted to `None`, e.g. "0000-00-00"
        """
        self._format = format
        self._zero_date = zero_date
    
    def process(self, value):
        from datetime import date, datetime
        if self._zero_date:
            zero_time = date(2000, 2, 2).strftime(self._format).replace('2', '0')
            if value == zero_time:
                return None
        if value is not None:
            return datetime.strptime(value, self._format).date()
    
class FormatDate(PipelineItem):
    def __init__(self, format='%Y-%m-%d', zero_date=False):
        """
        Format a datetime.date object as a string

        :param format: The format string to use when formatting
        :param zero_date: If `None` should be converted into a "zero date", e.g. "0000-00-00"
        """
        self._format = format
        self._zero_date = zero_date
    
    def process(self, value):
        from datetime import date
        if self._zero_date:
            zero_time = date(2000, 2, 2).strftime(self._format).replace('2', '0')
            if value is None:
                return zero_time
        if value is not None:
            return value.strftime(self._format)

class ParseBoolean(PipelineItem):
    def __init__(self, true_values={1, '1', 'true', 'True', 'TRUE', 'yes', 'Yes', 'YES', 'on', 'On', 'ON'}, false_values={0, '0', 'false', 'False', 'FALSE', 'no', 'No', 'NO', 'off', 'Off', 'OFF'}):
        self.true_values = set(true_values)
        self.false_values = set(false_values)
    
    def process(self, value):
        if value in self.true_values:
            return True
        if value in self.false_values:
            return False
        if value is None:
            return None
        raise ValueError(f'Unrecognized value: {repr(value)}')
        

class FormatBoolean(PipelineItem):
    def __init__(self, true_value='Yes', false_value='No'):
        self.true_value = true_value
        self.false_value = false_value
    
    def process(self, value):
        if value is None:
            return None
        if value:
            return self.true_value
        else:
            return self.false_value


class Lookup(PipelineItem):
    def __init__(self, lookup_map, *, convert_keys=None):
        """
        :param lookup_map: The dictionary to use as a lookup
        :param convert_keys: If provided, a callable that will be used to convert all lookup keys before
            use. May be useful when typing is inconsistent or more flexible typing is needed.Set to None
            to do no conversion.
        """
        if convert_keys is not None:
            lookup_map = {convert_keys(key):value for key,value in lookup_map.items()}
        self.convert_keys = convert_keys
        self.map = lookup_map
    
    def process(self, value):
        if self.convert_keys is not None:
            value = self.convert_keys(value)
        return self.map.get(value)

class StringReplace(PipelineItem):
    pass #TODO

class Default(PipelineItem):
    def __init__(self, value):
        self.value = value
    
    def process(self, value):
        if value is None:
            return self.value
        else:
            return value
    