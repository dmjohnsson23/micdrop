from .base import PipelineItem, Source, Put, OnFail
from time import time_ns
import os, shutil
from glob import iglob
from pathlib import Path
__all__ = ('WriteFile', 'CopyFile', 'MoveFile', 'ReadFile')


class WriteFile(PipelineItem):
    """
    Pipeline item that reroutes its input to a file, and supplies the filename as output.

    You can optionally control the name using the `put_name` method or the `name_pipeline` parameter.
    (These are equivalent). Otherwise, uses `time_ns()` to name the file.
    """
    def __init__(self, save_dir: str, is_binary=None, name_pipeline:Source = None, *, on_fail:OnFail = OnFail.fail):
        self.save_dir = save_dir
        self.is_binary = is_binary
        self.on_fail = OnFail(on_fail)
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
            name = self._put_name.guarded_get()
        name = os.path.join(self.save_dir, name)
        try:
            with open(name, 'wb' if is_binary else 'w') as file:
                file.write(value)
        except OSError as e:
            self.on_fail(e)
        return name
    

class _FilePipelineItemBase2(PipelineItem):
    """
    Base class for 2-file operations (move, copy)
    """
    def __init__(self, from_dir: str, to_dir: str, name_pipeline:Source = None, *, on_fail:OnFail = OnFail.fail):
        self.from_dir = from_dir
        self.to_dir = to_dir
        self.on_fail = OnFail(on_fail)
        self._put_name = name_pipeline
    
    def put_name(self):
        self._put_name = Put()
        return self._put_name
    
    def idempotent_next(self, idempotency_counter):
        super().idempotent_next(idempotency_counter)
        if self._put_name is not None:
            self._put_name.idempotent_next(idempotency_counter)


class CopyFile(_FilePipelineItemBase2):
    """
    Given an input filename, copy the file to a new location and return the new name
    """
    def process(self, value):
        if self._put_name is None:
            name = os.path.basename(value)
        else:
            name = self._put_name.guarded_get()
        name = os.path.join(self.to_dir, name)
        try:
            shutil.copy2(os.path.join(self.from_dir, value), name)
        except OSError as e:
            self.on_fail(e)
        return name
    

class MoveFile(_FilePipelineItemBase2):
    """
    Given an input filename, copy the file to a new location and return the new name
    """
    def process(self, value):
        if self._put_name is None:
            name = os.path.basename(value)
        else:
            name = self._put_name.guarded_get()
        name = os.path.join(self.to_dir, name)
        try:
            shutil.move(os.path.join(self.from_dir, value), name)
        except OSError as e:
            self.on_fail(e)
        return name


class ReadFile(PipelineItem):
    """
    Receives a filename as input and outputs the file contents.
    """
    def __init__(self, base_dir, is_binary=True, *open_args, on_fail:OnFail = OnFail.fail, **open_kwargs):
        self.base_dir = base_dir
        self.is_binary = is_binary
        self.on_fail = OnFail(on_fail)
        self.open_args = open_args
        self.open_kwargs = open_kwargs
    
    def process(self, value):
        try:
            with open(os.path.join(self.base_dir, value), 'rb' if self.is_binary else 'r') as file:
                return file.read()
        except OSError as e:
            self.on_fail(e)


class FilesSource(Source):
    """
    A source that yields the contents of files matching a glob pattern.

    This is most useful in conjunction with a pipeline item that actually parses the file contents, 
    such as::

        source = FilesSource('*.json') >> JsonParse()
    """
    def __init__(self, source, mode='r', **open_args):
        self._path = None
        self._value = None
        if isinstance(source, Path) and source.is_dir():
            self._iter = iter(source.iterdir())
        if isinstance(source, tuple):
            base, glob = source
            if not isinstance(base, Path):
                base = Path(base)
            self._iter = iter(base.glob(glob))
        else:
            self._iter = iter(iglob(source))
        self.mode = mode
        self.open_args = open_args
    
    def next(self):
        self._value = None
        self._path = next(self._iter)
    
    def get_index(self):
        return self._path
    
    def get(self):
        if self._value is None:
            with open(self._path, self.mode, **self.open_args) as file:
                self._value = file.read()
        return self._value