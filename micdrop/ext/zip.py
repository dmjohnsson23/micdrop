import os
import io
from time import time_ns
from zipfile import ZipFile as PyZipFile

from ..pipeline.files import FileOnFail
from ..pipeline.base import PipelineItem, Put, Source

# TODO ZipSource and ZipSink

class _ZipPipelineItem(PipelineItem):
    def __init__(self, zip, mode, zip_options, zip_class=PyZipFile):
        if isinstance(zip, zip_class):
            self.zip = zip
        else:
            self.zip = zip_class(zip, mode, **zip_options)


class WriteFileInZip(_ZipPipelineItem):
    """
    Pipeline item that reroutes its input to a file, and supplies the filename as output.

    You can optionally control the name using the `put_name` method or the `name_pipeline` parameter.
    (These are equivalent). Otherwise, uses `time_ns()` to name the file.
    """
    def __init__(self, zip, save_dir: str, is_binary=None, name_pipeline:Source = None, *, on_fail:FileOnFail = FileOnFail.fail):
        super().__init__(zip, 'a')
        self.save_dir = save_dir
        self.is_binary = is_binary
        self.on_fail = FileOnFail(on_fail)
        self._put_name = name_pipeline >> Put()
    
    def put_name(self):
        self._put_name = Put()
        return self._put_name

    def process(self, value):
        is_binary = self.is_binary
        if is_binary is None:
            is_binary = isinstance(value, (bytes, bytearray))
        if self._put_name is None:
            name = f"{time_ns()}.{'bin' if is_binary else 'txt'}"
        else:
            name = self._put_name.get()
        name = os.path.join(self.save_dir, name)
        try:
            if is_binary:
                with self.zip.open(name, 'w') as file:
                    file.write(value)
            else:
                with io.TextIOWrapper(self.zip.open(name, 'w')) as file:
                    file.write(value)
        except IOError as e:
            self.on_fail(e)
        return name

class ReadFileInZip(_ZipPipelineItem):
    """
    Receives a filename as input and outputs the file contents.
    """
    def __init__(self, zip, is_binary=True, *open_args, on_fail:FileOnFail = FileOnFail.fail, **open_kwargs):
        super().__init__(zip, 'r')
        self.is_binary = is_binary
        self.on_fail = FileOnFail(on_fail)
        self.open_args = open_args
        self.open_kwargs = open_kwargs
    
    def process(self, value):
        try:
            if self.is_binary:
                with self.zip.open(value, 'r') as file:
                    return file.read()
            else:
                with io.TextIOWrapper(self.zip.open(value, 'r')) as file:
                    return file.read()
        except IOError as e:
            self.on_fail(e)


class ExtractFileFromZip(_ZipPipelineItem):
    """
    Receives a filename as input, and extracts that file to a given directory
    """
    def __init__(self, zip, to_dir: str, name_pipeline:Source = None, *, on_fail:FileOnFail = FileOnFail.fail):
        super().__init__(zip, 'r')
        self.to_dir = to_dir
        self.on_fail = FileOnFail(on_fail)
        self._put_name = name_pipeline
    
    def put_name(self):
        self._put_name = Put()
        return self._put_name
    
    def process(self, value):
        if self._put_name is None:
            name = os.path.basename(value)
        else:
            name = self._put_name.get()
        name = os.path.join(self.to_dir, name)
        try:
            self.zip.extract(value, name)
        except Exception as e:
            self.on_fail(e)
        return name