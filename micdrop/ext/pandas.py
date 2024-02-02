"""
An extension for Micdrop creating integrations with Pandas, allowing us to take advantage of its 
numerous import and export formats.
"""
from ..pipeline import Source
from ..sink import Sink
import pandas as pd


class DataFrameSource(Source):
    """
    A source that iterates over the rows of a pandas dataframe.

    Example::

        source = DataFrameSource(pd.read_excel('data.xlsx'))
    """
    _current_row = None
    _current_index = None
    
    def __init__(self, dataframe:pd.DataFrame) -> None:
        self._progress_total = len(dataframe.index)
        self._dataframe = dataframe
        self._iterable = iter(dataframe.iterrows())

    def get(self):
        return self._current_row
            
    def next(self):
        self._current_index. self._current_row = next(self._iterable) # Deliberately allow StopIteration to propagate

class DataFrameSink(Sink):
    """
    A sink that will write its output to a dataframe, which you can then transform further and/or 
    export using pandas.

    Processing will yield a series of `pd.Series` objects. After processing is done, the final 
    `pd.DataFrame` object can be retrieved from the `dataframe` property.

    Example::

        sink = DataFrameSink()
        #...map items from source to sink here...
        process_all(sink)
        sink.dataframe.to_excel('data.xlsx')
    """
    dataframe: pd.DataFrame
    def __init__(self, initial:pd.DataFrame=None):
        """
        :param initial: An optional initial value for the dataframe, for if you want to append to
        an existing dataframe.
        """
        self.dataframe = initial

    def get(self):
        s = pd.Series(super().get())
        if self.dataframe is None:
            self.dataframe = s.to_frame().T
        else:
            self.dataframe = pd.concat([self.dataframe, s.to_frame().T], ignore_index=True, copy=False)
        return s
