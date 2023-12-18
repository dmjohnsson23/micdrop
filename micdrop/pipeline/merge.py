from typing import Sequence, Callable, Mapping, Any

from .base import Source, Put

class OnConflict:
    @staticmethod
    def first(values:Sequence):
        """
        Always return the first value, no matter what
        """
        if values:
            return values[0]
        else:
            return None
    
    @staticmethod
    def last(values:Sequence):
        """
        Always return the last value, no matter what
        """
        if values:
            return values[-1]
        else:
            return None
        
    @staticmethod
    def first_not_none(values:Sequence):
        """
        Return the first value that is not None
        """
        for value in values:
            if value is not None:
                return value
        return None
    
    @staticmethod
    def last_not_none(values:Sequence):
        """
        Return the last value that is not None
        """
        for value in reversed(values):
            if value is not None:
                return value
        return None
    
    @staticmethod
    def first_truthy(values:Sequence):
        """
        Return the first value that is truthy
        """
        for value in values:
            if value:
                return value
        return None
    
    @staticmethod
    def last_truthy(values:Sequence):
        """
        Return the last value that is truthy
        """
        for value in reversed(values):
            if value:
                return value
        return None
    
    @staticmethod
    def least_not_none(values:Sequence):
        """
        Return the lowest value that is not None
        """
        return min([v for v in values if v is not None])
    
    @staticmethod
    def greatest_not_none(values:Sequence):
        """
        Return the lowest value that is not None
        """
        return min([v for v in values if v is not None])
    
    @staticmethod
    def sum(values:Sequence):
        """
        Add conflicting values together
        """
        return sum([v for v in values if v is not None])
    
    @staticmethod
    def concatenate(delimiter:str=''):
        """
        Return a function that will concatenate conflicting values together with the given delimiter
        """
        def concatenate(values:Sequence):
            """
            Concatenate conflicting values together
            """
            return delimiter.join([v for v in values if v is not None])
        return concatenate
    
    @staticmethod
    def n_way_first(values:Sequence):
        """
        Assumes the first value is the original, and all others are variants. Return the first 
        variant value that is not equal to the original value. If all values are equal to the 
        original, return the original.
        """
        if not values:
            return None
        original = values[0]
        variants = values[1:]
        for variant in variants:
            if variant != original:
                return variant
        return original
    
    @staticmethod
    def n_way_last(values:Sequence):
        """
        Assumes the first value is the original, and all others are variants. Return the last 
        variant value that is not equal to the original value. If all values are equal to the 
        original, return the original.
        """
        if not values:
            return None
        original = values[0]
        variants = values[1:]
        for variant in reversed(variants):
            if variant != original:
                return variant
        return original


class MergeDicts(Source):
    """
    Collect multiple pipelines, all of which should produce a dict, and merge the dicts according 
    to merger rules.

    Examples::

        # Context Manager Syntax
        with MergeDicts(
            sort_key=lambda d: d['timestamp'], 
            default_conflict_resolver=OnConflict.last_not_none
        ) as collect:
            source.take('dict1') >> merge.put()
            source.take('dict2') >> merge.put()
            merge >> sink.put('merged_dict')

        # Callable Syntax
        MergeDicts(
            source.take('dict1'),
            source.take('dict2'),
            sort_key=lambda d: d['timestamp'], 
            default_conflict_resolver=OnConflict.last_not_none
        ) >> sink.put('merged_dict')
    
    This class is intended for use merging two sources with the same data structure, like the 
    following example using the sqlalchemy extension::

        # Source and sink are the same table
        source = TableSource(engine, 'data')
        sink = TableSink(engine, 'data')

        MergeDicts(
            source,
            # Get the matching value from the table to be merged
            source.take('id') >> FetchRow(engine, 'other_data', 'id'),
            # Favor values from the newest of the two records
            sort_key=lambda d: d['last_modified'], 
            default_conflict_resolver=OnConflict.last_not_none
        ) >> sink

        process_all(sink)
    """
    _merged: dict = None
    def __init__(self, *pipelines:Source, sort_key:Callable[[dict],Any]=None, sort_reversed:bool=False, default_conflict_resolver:Callable[[Sequence],Any] = OnConflict.first_not_none, conflict_resolvers:Mapping[Any,Callable[[Sequence],Any]]={}, keys=None):
        """
        :param pipelines: The pipelines to provide the source dicts. If not provided here, you can 
            also use the `put` method to provide them.
        :param sort_key: If provided, the source dicts will be sorted using this key before 
            applying the conflict resolvers. You could use this, for example, to favor newer 
            items over older ones.
        :param sort_reversed: If true, `sort_key` will be reversed.
        :param conflict_resolvers: A dictionary of conflict resolvers. The keys of this dictionary 
            should match the keys of the source dictionary, and the values should be functions 
            which will take a list of values and return a single value from the list. The 
            `OnConflict` class in this module provides several such functions.
        :param default_conflict_resolver: The conflict resolver function to use if a matching 
            resolver for the dictionary key is not found in `conflict_resolvers`
        :param keys: If provided, a preset list of keys. Keys not in this list will be ignored for 
            all source dictionaries.
        """
        self._puts = [item >> Put() for item in pipelines]
        self.sort_key = sort_key
        self.sort_reversed = sort_reversed
        self.default_conflict_resolver = default_conflict_resolver
        self.conflict_resolvers = conflict_resolvers
        self.keys = keys
    
    def get(self):
        if self._merged is None:
            self._merged = {}
            dicts = [put.get() for put in self._puts]
            if self.sort_key is not None:
                dicts = list(sorted(dicts, key=self.sort_key, reverse=self.sort_reversed))
            if self.keys is not None:
                keys = self.keys
            else:
                keys = set()
                for d in dicts:
                    keys.update(d.keys())
            for key in keys:
                values = []
                for d in dicts:
                    if key in d:
                        values.append(d[key])
                if len(values) == 1:
                    self._merged[key] = values[0]
                elif key in self.conflict_resolvers:
                    self._merged[key] = self.conflict_resolvers[key](values)
                else:
                    self._merged[key] = self.default_conflict_resolver(values)
        return self._merged
    
    def next(self):
        self._merged = None

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
