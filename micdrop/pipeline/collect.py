from .base import PipelineSource, Put, PipelineItem
__all__ = ('CollectDict', 'CollectList', 'CollectArgsKwargs', 'CollectArgsKwargsTakeMixin', 'CollectFormatString', 'CollectCall', 'CollectValueOther')

class CollectDict(PipelineSource):
    _dict: dict = None
    def __init__(self, **pipelines:PipelineSource):
        self._puts = {key: item >> Put() for key, item in pipelines.items()}
    
    def get(self):
        if self._dict is None:
            self._dict = {key: put.get() for key, put in self._puts.items()}
        return self._dict
    
    def reset(self):
        self._dict = None
        for put in self._puts.values():
            put.reset()
    
    def put(self, key):
        put = Put()
        self._puts[key] = put
        return put


class CollectList(PipelineSource):
    _list: list = None
    def __init__(self, *pipelines:PipelineSource):
        self._puts = [item >> Put() for item in pipelines]
    
    def get(self):
        if self._list is None:
            self._list = [put.get() for put in self._puts]
        return self._list
    
    def reset(self):
        self._list = None
        for put in self._puts:
            put.reset()
    
    def put(self):
        put = Put()
        self._puts.append(put)
        return put


class CollectArgsKwargs(PipelineSource):
    """
    Base class for collectors that want to allow both named and unnamed puts; do not use directly
    """
    def __init__(self, *args_pipelines:PipelineSource, **kwargs_pipelines:PipelineSource):
        self._args = [item >> Put() for item in args_pipelines]
        self._kwargs = {key: item >> Put() for key, item in kwargs_pipelines.items()}
    
    def get(self):
        return (
            [put.get() for put in self._args],
            {key: put.get() for key, put in self._kwargs.items()}
        )
    
    def put(self, key=None):
        put = Put()
        if key is None:
            self._args.append(put)
        else:
            self._kwargs[key] = put
        return put
    
    
    def reset(self):
        for put in self._args:
            put.reset()
        for put in self._kwargs.values():
            put.reset()

class CollectArgsKwargsTakeMixin:
    _auto_key = 0
    def take(self, key=None) -> PipelineSource:
        """
        Take a sub-value from the pipeline; used to split fields or destructure data.
        """
        if key is None:
            key = self._auto_key
            self._auto_key += 1
        return self >> TakeArgsKwargs(key)
    
class TakeArgsKwargs(PipelineItem):
    _key = None

    def __init__(self, key):
        self._key = key
    
    def reset(self):
        super().reset()
    
    def get(self):
        args, kwargs = self._prev.get()
        if isinstance(self._key, int):
            return args[self._key]
        else:
            return kwargs[self._key]

class CollectFormatString(CollectArgsKwargs):
    """
    Collector that allows using put operations to populate fields in a format string
    """
    _value = None
    def __init__(self, format_string:str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.format_string = format_string

    def get(self):
        if self._value is None:
            args, kwargs = super().get()
            self._value = self.format_string.format(*args, **kwargs)
        return self._value
    
    def reset(self):
        super().reset()
        self._value = None


class CollectCall(CollectArgsKwargs):
    """
    Collector that allows using put operations to populate arguments to a function or callable
    """
    _value = None
    def __init__(self, func, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.func = func

    def get(self):
        if self._value is None:
            args, kwargs = super().get()
            self._value = self.func(*args, **kwargs)
        return self._value
    
    def reset(self):
        super().reset()
        self._value = None


class CollectValueOther(PipelineSource):
    """
    Special collector used to handle the common use case of mapping two fields at once: some 
    field with a limited set of predefined values, and another free-text field for "other" values.

    Example usage::

        with CollectValueOther() as collector:
            # We put three values: the fully mapped value... (to use when a proper mapping exists)
            source.get('value') >> Lookup(mapping) >> collector.put_mapped()
            # ...the raw value... (to prepend to the other value if the mapped value is None)
            source.get('value') >> collector.put_unmapped()
            # ...and the original other value.
            source.get('other') >> collector.put_other()
            # Then we can take the mapped and other value
            collector.take_value() >> Default('Other') >> sink.put('value')
            collector.take_other() >> sink.put('other')
    """
    _value = None
    _other = None
    _cached = False
    def __init__(self, delimiter=': '):
        self._puts = (Put(), Put(), Put())
        self.delimiter = delimiter
    
    def get(self):
        if not self._cached:
            p_mapped, p_unmapped, p_other = self._puts
            mapped = p_mapped.get()
            unmapped = p_unmapped.get()
            other = p_other.get()
            self._value = mapped
            if mapped is None and other is None:
                self._other = unmapped
            elif mapped is None:
                self._other = f"{unmapped}{self.delimiter}{other}"
            else:
                self._other = other
            self._cached = True
        return self._value, self._other
    
    def reset(self):
        self._value = None
        self._other = None
        self._cached = False
    
    def put_mapped(self):
        """
        Put the mapped value. This should be `None` if no mapping exists and "Other" should be used.
        This value is then retrieved unchanged with `take_value`.
        """
        return self._puts[0]
    
    def put_unmapped(self):
        """
        Put the unmapped value. This will be prepended to the "Other" value if the mapped value is `None`.
        Should be a human-readable string representation.
        """
        return self._puts[1]
    
    def put_other(self):
        """
        Put the other value. Should be a string.
        """
        return self._puts[2]
    
    def take_value(self, default=None):
        """
        Take the value. This will be the same value that was put with `put_mapped`. You'll probably want to add
        a `Default` after this take.
        """
        take = self.take(0)
        if default is not None:
            from .transformers import Default
            take = take >> Default(default)
        return take
    
    def take_other(self):
        """
        Retrieve the "Other" string.
        """
        return self.take(1)