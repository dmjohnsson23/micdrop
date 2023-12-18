from .base import PipelineItem
from ..utils.deferred_operand import DeferredOperandConstructorValueMeta

__all__ = ('Filter','FilterDictKeys')

class Filter(PipelineItem, DeferredOperandConstructorValueMeta):
    """
    Pipeline item analogous to Python's built-in `filter` function.
    """
    def __init__(self, func=None):
        self.func = func
    
    def process(self, value):
        return tuple(filter(self.func, value))
    
class FilterDictKeys(PipelineItem, DeferredOperandConstructorValueMeta):
    """
    A filter for dict objects based on keys.
    """
    def __init__(self, func=None):
        self.func = func
    
    def process(self, value):
        return {key:value[key] for key in filter(self.func, value.keys())}