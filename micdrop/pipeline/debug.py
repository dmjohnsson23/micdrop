from .base import PipelineItem

class InspectPrint(PipelineItem):
    """
    Does not change the pipeline value in any way, but prints it out when called.
    """
    def __init__(self, *print_args, **print_kwargs):
        self._print_args = print_args
        self._print_kwargs = print_kwargs
    
    def keys(self):
        return self._prev.keys()

    def get(self):
        value = self._prev.guarded_get()
        print(*self._print_args, value, **self._print_kwargs)
        return value