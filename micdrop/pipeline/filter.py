from .base import PipelineItem
from ..utils.deferred_operand import DeferredOperandConstructorValueMeta

__all__ = ('Filter','FilterDictKeys')

class Filter(PipelineItem, metaclass=DeferredOperandConstructorValueMeta):
    """
    Pipeline item analogous to Python's built-in `filter` function.

    Example::

        # Filter out "falsy" items
        source.take('items') >> Filter() >> sink.put('true_items')
        # Filter based on custom criteria
        source.take('items') >> Filter(lambda item: item > 99) >> sink.put('big_items')
        # Alternate syntax
        source.take('items') >> (Filter.value > 99) >> sink.put('big_items')
    """
    def __init__(self, func=None):
        self.func = func
    
    def process(self, value):
        return tuple(filter(self.func, value))
    
class FilterDictKeys(PipelineItem, metaclass=DeferredOperandConstructorValueMeta):
    """
    A filter for dict objects based on keys.

    Example::

        source.take('dict') >> FilterDictKeys(lambda key: key != 'id') >> sink.put('dict')
        # Alternate syntax (note that `value` is a deferred operation on the key)
        source.take('dict') >> (FilterDictKeys.value != 'id') >> sink.put('dict')
    """
    def __init__(self, func=None):
        self.func = func
    
    def process(self, value):
        return {key:value[key] for key in filter(self.func, value.keys())}