"""
Extension integrating with the csv module in the python standard library.
"""
from ..source import Source
from ..sink import Sink
from csv import DictReader, DictWriter
from io import IOBase

class CSVSource(Source):
    """
    Allows using a CSV file as a source. Uses `csv.DictReader` under the hood.
    """
    def __init__(self, file, *, encoding=None, **csv_reader_options):
        super().__init__()
        self._file = file
        self._encoding = encoding
        self._reader_opts = csv_reader_options
        self._csv = None

    def get(self, key):
        return self._current_value[key]

    def next(self):
        try:
            self._current_value = next(self._csv)
            self._current_index += 1
            return True
        except StopIteration:
            return False
    
    def open(self):
        if not isinstance(self._file, IOBase):
            self._file = open(self._file, 'r', newline='', encoding=self._encoding)
        self._csv = iter(DictReader(
            map(lambda line: line.replace('\0', ''), self._file), # Strip any null bytes in the string (I've found these in some malformed CSVs)
            **self._reader_opts))
        self._current_index = -1
    
    def close(self):
        self._file.close()


class CSVSink(Sink):
    """
    Allows outputting to a CSV file as a sink. Uses `csv.DictWriter` under the hood.
    """
    def __init__(self, file, *, encoding=None, **csv_writer_options):
        super().__init__()
        self._file = file
        self._encoding = encoding
        self._writer_opts = csv_writer_options
        self._csv = None
    
    def process(self, source, *, write_header=True):
        if not isinstance(self._file, IOBase):
            self._file = open(self._file, 'w', newline='', encoding=self._encoding)
        if 'fieldnames' not in self._writer_opts:
            self._writer_opts['fieldnames'] = self._puts.keys() 
        writer = DictWriter(self._file, **self._writer_opts)
        if write_header:
            writer.writeheader()
        for row in super().process(source):
            writer.writerow(row)
            yield row


