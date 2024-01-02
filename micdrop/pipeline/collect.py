from .base import Source, Put, PipelineItem
from itertools import chain
__all__ = ('CollectDict', 'CollectList', 'CollectArgsKwargs', 'CollectArgsKwargsTakeMixin', 'CollectFormatString', 'CollectCall', 'CollectValueOther')

class CollectDict(Source):
    """
    Collect multiple pipelines into a dict

    Examples::

        # Context Manager Syntax
        with CollectDict() as collect:
            source.take('thing1') >> collect.put('thing1')
            source.take('thing2') >> collect.put('thing2')
            collect >> sink.put('things')

        # Callable Syntax
        CollectDict(
            thing1 = source.take('thing1'),
            thing2 = source.take('thing2'),
        ) >> sink.put('things')
    """
    _dict: dict = None
    def __init__(self, **pipelines:Source):
        self._puts = {key: item >> Put() for key, item in pipelines.items()}
    
    def keys(self):
        return self._puts.keys()
    
    def get(self):
        if self._dict is None:
            self._dict = {key: put.guarded_get() for key, put in self._puts.items()}
        return self._dict
    
    def next(self):
        self._dict = None

    def idempotent_next(self, idempotency_counter):
        super().idempotent_next(idempotency_counter)
        for put in self._puts.values():
            put.idempotent_next(idempotency_counter)
    
    def put(self, key):
        put = Put()
        self._puts[key] = put
        return put

    def open(self):
        for put in self._puts.values():
            if not put.is_open:
                put.open()
        super().open()

    def close(self):
        for put in self._puts.values():
            if put.is_open:
                put.close()
        super().close()


class CollectList(Source):
    """
    Collect multiple pipelines into a list

    Examples::

        # Context Manager Syntax
        with CollectList() as collect:
            source.take('thing1') >> collect.put()
            source.take('thing2') >> collect.put()
            collect >> sink.put('things')

        # Callable Syntax
        CollectList(
            source.take('thing1'),
            source.take('thing2'),
        ) >> sink.put('things')
    """
    _list: list = None
    def __init__(self, *pipelines:Source):
        self._puts = [item >> Put() for item in pipelines]
    
    def keys(self):
        return range(len(self._puts))
    
    def get(self):
        if self._list is None:
            self._list = [put.guarded_get() for put in self._puts]
        return self._list
    
    def next(self):
        self._list = None

    def idempotent_next(self, idempotency_counter):
        super().idempotent_next(idempotency_counter)
        for put in self._puts:
            put.idempotent_next(idempotency_counter)
    
    def put(self):
        put = Put()
        self._puts.append(put)
        return put

    def open(self):
        for put in self._puts:
            if not put.is_open:
                put.open()
        super().open()

    def close(self):
        for put in self._puts:
            if put.is_open:
                put.close()
        super().close()


class CollectArgsKwargs(Source):
    """
    Base class for collectors that want to allow both named and unnamed puts; do not use directly
    """
    def __init__(self, *args_pipelines:Source, **kwargs_pipelines:Source):
        self._args = [item >> Put() for item in args_pipelines]
        self._kwargs = {key: item >> Put() for key, item in kwargs_pipelines.items()}
    
    def get(self):
        return (
            [put.guarded_get() for put in self._args],
            {key: put.guarded_get() for key, put in self._kwargs.items()}
        )
    
    def put(self, key=None):
        put = Put()
        if key is None:
            self._args.append(put)
        else:
            self._kwargs[key] = put
        return put
    
    def idempotent_next(self, idempotency_counter):
        super().idempotent_next(idempotency_counter)
        for put in self._args:
            put.idempotent_next(idempotency_counter)
        for put in self._kwargs.values():
            put.idempotent_next(idempotency_counter)

    def open(self):
        for put in self._args:
            if not put.is_open:
                put.open()
        for put in self._kwargs.values():
            if not put.is_open:
                put.open()
        super().open()

    def close(self):
        for put in self._args:
            if put.is_open:
                put.close()
        for put in self._kwargs.values():
            if put.is_open:
                put.close()
        super().close()


class CollectArgsKwargsTakeMixin:
    _auto_key = 0
    def keys(self):
        return chain(range(len(self._args)), self._kwargs.keys())
    
    def take(self, key=None) -> Source:
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
    
    def get(self):
        args, kwargs = self._prev.guarded_get()
        if isinstance(self._key, int):
            return args[self._key]
        else:
            return kwargs[self._key]

class CollectFormatString(CollectArgsKwargs):
    """
    Collector that allows using put operations to populate fields in a format string.

    Keyword values are added with ``collect_fstr.put('arg_name')``, positional values are added 
    with ``collect_fstr.put()``. You are free to mix-and-match the two.

    Examples::

        # Context Manager Syntax
        with CollectFormatString("Does {person} like {thing}?") as collect:
            source.take('person') >> collect.put('person')
            source.take('thing') >> collect.put('thing')
            collect >> sink.put('question')
        
        # Callable Syntax
        CollectFormatString("Does {person} like {thing}?"
            person = source.take('person'),
            thing = source.take('thing'),
        ) >> sink.put('question')
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
    
    def next(self):
        super().next()
        self._value = None


class CollectCall(CollectArgsKwargs):
    """
    Collector that allows using put operations to populate arguments to a function or callable.

    Keyword args are added with ``collect_call.put('arg_name')``, positional args are added with
    ``collect_call.put()``. You are free to mix-and-match the two.

    Examples (using four different equivalent syntaxes)::

        # Decorator syntax
        @CollectCall.of(source.take('thing1'), source.take('thing2'))
        def do_things(thing1, thing2):
            return thing1 + thing2
        do_things >> sink.put('things')

        # Alternate decorator syntax
        @CollectCall
        def do_things(thing1, thing2):
            return thing1 + thing2
        source.take('thing1') >> do_things.put('thing1')
        source.take('thing2') >> do_things.put('thing2')
        do_things >> sink.put('things')

        # Context Manager Syntax  
        def do_things(thing1, thing2):
            return thing1 + thing2
        with CollectCall(do_things) as collect:
            source.take('thing1') >> collect.put()
            source.take('thing2') >> collect.put()
            collect >> sink.put('things')
        
        # Callable syntax
        def do_things(thing1, thing2):
            return thing1 + thing2
        CollectCall(do_things,
            thing1 = source.take('thing1'),
            thing2 = source.take('thing2'),
        ) >> sink.put('things')

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
    
    def next(self):
        super().next()
        self._value = None
    
    @classmethod
    def of(cls, *args, **kwargs):
        """
        Special decorator syntax for the collect call::

            @CollectCall.of(source.take('thing1'), source.take('thing2'))
            def do_things(thing1, thing2):
                return thing1 + thing2
            do_things >> sink.put('things')
        """
        def wrapper(f):
            return cls(f, *args, **kwargs)
        return wrapper


class CollectValueOther(Source):
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
            mapped = p_mapped.guarded_get()
            unmapped = p_unmapped.guarded_get()
            other = p_other.guarded_get()
            self._value = mapped
            if mapped is None and other is None:
                self._other = unmapped
            elif mapped is None:
                self._other = f"{unmapped}{self.delimiter}{other}"
            else:
                self._other = other
            self._cached = True
        return self._value, self._other
    
    def next(self):
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

    def open(self):
        for put in self._puts:
            if not put.is_open:
                put.open()
        super().open()

    def close(self):
        for put in self._puts:
            if put.is_open:
                put.close()
        super().close()