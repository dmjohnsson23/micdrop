from .base import PipelineItem

class InspectPrint(PipelineItem):
    """
    Does not change the pipeline value in any way, but prints it out when called.
    """
    def get(self):
        value = self._prev.get()
        print(value)
        return value