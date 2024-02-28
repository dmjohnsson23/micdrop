from ..pipeline import PipelineItem, Source
import pandoc
from typing import List

class PandocConvert(PipelineItem):
    def __init__(self, input_format:str, output_format:str, input_is_file:bool=False, output_filename_pipeline:Source=None, input_options:List[str]=None, output_options:List[str]=None):
        self.input_format = input_format
        self.input_is_file = input_is_file
        self.input_options = input_options
        self.output_format = output_format
        self.output_filename_pipeline = output_filename_pipeline
        self.output_options = output_options
    
    def process(self, value):
        if self.input_is_file:
            doc = pandoc.read(None, value, self.input_format, self.input_options)
        else:
            doc = pandoc.read(value, None, self.input_format, self.input_options)
        if self.output_filename_pipeline is None:
            return pandoc.write(doc, None, self.output_format, self.output_options)
        else:
            filename = self.output_filename_pipeline.get()
            pandoc.write(doc, filename, self.output_format, self.output_options)
            return filename
    
    def idempotent_next(self, idempotency_counter):
        super().idempotent_next(idempotency_counter)
        if self.output_filename_pipeline is not None:
            self.output_filename_pipeline.idempotent_next(idempotency_counter)