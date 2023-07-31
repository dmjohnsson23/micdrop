"""
Extension integrating with the csv module in the python standard library.
"""
from ..source import Source
from ..sink import Sink
from csv import DictReader, DictWriter
from io import IOBase

class CSVSource(Source):
    def __init__(self, file, *, encoding=None, **csv_reader_options):
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
        self.file.close()


class CSVSink(Sink):
    def __init__(self, file, *, encoding=None, **csv_writer_options):
        self._file = file
        self._encoding = encoding
        self._writer_opts = csv_writer_options
        self._csv = None
    
    def process(self, source):
        if not isinstance(self._file, IOBase):
            self._file = open(self._file, 'w', newline='', encoding=self._encoding)
        if 'fieldnames' not in self._writer_opts:
            self._writer_opts['fieldnames'] = self._puts.keys() 
        writer = DictWriter(self._file, **self._writer_opts)
        writer.writeheader()
        for row in super().process(source):
            writer.write(row)


