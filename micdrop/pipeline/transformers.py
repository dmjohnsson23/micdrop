"""
Collection of pipeline items that perform various transformation
"""
__all__ = ('ParseDatetime', 'FormatDatetime', 'ParseDate', 'FormatDate')

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