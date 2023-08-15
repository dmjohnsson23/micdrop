from __future__ import annotations
from .base import PipelineItem, PipelineSink, PipelineSource, Call


class ProxySource(PipelineSource):
    """
    A "pseudo" source that is intended to be puppeteered by another object
    """
    _value = None

    def get(self):
        return self._value
    
    def reset(self):
        self._value = None
    
    def set(self, value):
        self._value = value

class PipelineSegment:
    """
    A reusable piece of a pipeline
    """
    _inlet_proxy = ProxySource = None
    _inlet: PipelineSource = None
    _outlet: PipelineSink = None

    def __rshift__(self, next):
        if self._outlet is None:
            next = PipelineSink.create(next)
            self.set_inlet(next)
            self.set_outlet(next)
        else:
            self.set_outlet(self._outlet >> next)
        return self
    
    def __lshift__(self, prev):
        if self._inlet is None:
            prev = PipelineSource.create(prev)
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
    
    def set_inlet(self, inlet: PipelineSource):
        """
        Manually set the inlet of this pipeline segment. 
        
        This should rarely be used under normal circumstances, but may be necessary for more complex branching pipelines.
        """
        inlet = PipelineSource.create(inlet)
        proxy = ProxySource()
        proxy >> inlet
        self._inlet = inlet
        self._inlet_proxy = proxy
    
    def set_outlet(self, outlet: PipelineSink):
        """
        Manually set the outlet of this pipeline segment. 
        
        This should rarely be used under normal circumstances, but may be necessary for more complex branching pipelines.
        """
        outlet = PipelineSink.create(outlet)
        self._outlet = outlet
    
    def apply(self) -> AppliedPipelineSegment:
        """
        Get a `PipelineItem` that will run values through this pipeline segment
        """
        return AppliedPipelineSegment(self)

    to_pipeline_item = apply


class AppliedPipelineSegment(PipelineItem):
    _segment: PipelineSegment

    def __init__(self, segment:PipelineSegment):
        self._segment = segment
    
    def process(self, value):
        self._segment._outlet.reset()
        self._segment._inlet_proxy.set(value)
        return self._segment._outlet.get()