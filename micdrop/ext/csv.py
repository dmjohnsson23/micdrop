"""
Extension integrating with the csv module in the python standard library.
"""
from ..pipeline import Source, logger, PipelineItem, Lookup
from ..sink import Sink
from csv import DictReader, DictWriter
from io import IOBase

class CSVSource(Source):
    """
    Allows using a CSV file as a source. Uses `csv.DictReader` under the hood.
    """
    def __init__(self, file, *, encoding=None, empty_is_none=False, **csv_reader_options):
        super().__init__()
        self._file = file
        self._encoding = encoding
        self._reader_opts = csv_reader_options
        self._reader = None
        self._iter = None
        self._empty_is_none = empty_is_none
    
    def keys(self):
        if not self.is_open:
            self.open()
        return self._reader.fieldnames

    def next(self):
        self._current_value = next(self._iter) # Deliberately allow StopIteration to propagate
        if self._empty_is_none:
            for key, value in self._current_value.items():
                if value == '':
                    self._current_value[key] = None
        logger.info('Next value: %s', self._current_value)
        self._current_index += 1
    
    def get(self):
        return self._current_value
    
    def get_index(self):
        return self._current_index
    
    def open(self):
        super().open()
        if not isinstance(self._file, IOBase):
            self._file = open(self._file, 'r', newline='', encoding=self._encoding)
        self._reader = DictReader(
            map(lambda line: line.replace('\0', ''), self._file), # Strip any null bytes in the string (I've found these in some malformed CSVs)
            **self._reader_opts)
        self._iter = iter(self._reader)
        self._current_index = -1
    
    def close(self):
        super().close()
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
            self._writer_opts['fieldnames'] = self.keys() 
        writer = DictWriter(self._file, **self._writer_opts)
        if write_header:
            writer.writeheader()
        for row in super().process(source):
            writer.writerow(row)
            yield row


class CsvLookup(Lookup):
    """
    Load a lookup table from a CSV file
    """
    def __init__(self, file, key_column=None, value_column=None, *, encoding=None, convert_keys=None, pass_if_not_found=False, **csv_reader_options):
        if not isinstance(self._file, IOBase):
            file = open(self._file, 'r', newline='', encoding=encoding)
        with file:
            with DictReader(file, **csv_reader_options) as reader:
                mapping = {}
                for index, row in enumerate(reader):
                    key = row[key_column] if key_column is not None else index
                    value = row[value_column] if value_column is not None else row
                    mapping[key] = value
        super().__init__(mapping, convert_keys=convert_keys, pass_if_not_found=pass_if_not_found)
        