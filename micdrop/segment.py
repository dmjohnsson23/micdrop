from __future__ import annotations
from .base import PipelineItem, Put, Source, Call
__all__ = ('ProxySource', 'PipelineSegment', 'AppliedPipelineSegment')

class ProxySource(Source):
    """
    A "pseudo" source that is intended to be puppeteered by another object
    """
    _value = None

    def get(self):
        return self._value
    
    def next(self):
        self._value = None
    
    def set(self, value):
        self._value = value

class PipelineSegment:
    """
    A reusable piece of a pipeline
    """
    _inlet_proxy = ProxySource = None
    _inlet: Source = None
    _outlet: Put = None
    _apply_counter: int = 0

    def __rshift__(self, next):
        if self._outlet is None:
            next = Put.create(next)
            self.set_inlet(next)
            self.set_outlet(next)
        else:
            self.set_outlet(self._outlet >> next)
        return self
    
    def __lshift__(self, prev):
        if self._inlet is None:
            prev = Source.create(prev)
            self.set_inlet(prev)
            self.set_outlet(prev)
        else:
            self.set_inlet(self._inlet << prev)
        return self
    
    def __call__(self, func):
        if self._outlet is None:
            call = Call(func)
            self.set_inlet(call)
            self.set_outlet(call)
        else:
            self.set_outlet(self._outlet(func))
        return self
    
    def set_inlet(self, inlet: Source):
        """
        Manually set the inlet of this pipeline segment. 
        
        This should rarely be used under normal circumstances, but may be necessary for more complex branching pipelines.
        """
        inlet = Source.create(inlet)
        proxy = ProxySource()
        proxy >> inlet
        self._inlet = inlet
        self._inlet_proxy = proxy
    
    def set_outlet(self, outlet: Put):
        """
        Manually set the outlet of this pipeline segment. 
        
        This should rarely be used under normal circumstances, but may be necessary for more complex branching pipelines.
        """
        outlet = Put.create(outlet)
        self._outlet = outlet
    
    def apply(self) -> AppliedPipelineSegment:
        """
        Get a `PipelineItem` that will run values through this pipeline segment
        """
        self._apply_counter += 1
        return AppliedPipelineSegment(self, self._apply_counter)

    to_pipeline_item = apply


class AppliedPipelineSegment(PipelineItem):
    _segment: PipelineSegment

    def __init__(self, segment:PipelineSegment, applied_id):
        self._segment = segment
        self._applied_id = applied_id
    
    def process(self, value):
        # we run `next` here to insure that the segment can be used multiple times in the same pipeline
        self._segment._outlet.idempotent_next((self._applied_id, self._reset_idempotency))
        self._segment._inlet_proxy.set(value)
        return self._segment._outlet.get()