from .base import OnFail, Source
from ..exceptions import SkipRowException, StopProcessingException
from typing import Callable, Sequence
__all__ = ('FilteredSource','RepeaterSource')

class FilteredSource(Source):
    """
    A wrapper around another source to filter certain rows.
    """
    def __init__(self, source:Source, condition:Callable):
        """
        :param source: The source to wrap
        :param condition: The function to call to check each row; will receive the row as its only parameter
        """
        self.source = source
        self.condition = condition

    def idempotent_next(self, idempotency_counter):
        counter = 0
        while True:
            try:
                self.source.idempotent_next((counter, idempotency_counter))
                if self.condition(self.source.guarded_get()):
                    return
            except SkipRowException:
                continue
            except (StopIteration, StopProcessingException):
                break
    
    def get(self):
        return self.source.get()
    
    def open(self):
        return self.source.open()

    def close(self):
        return self.source.close()
    

class RepeaterSource(Source):
    """
    Similar to `RepeaterSink`, except that it operates on the source side rather than the sink side.

    Example::

        source = RepeaterSource(other_source)

        source.take('normal_column') >> sink.put('normal_column')
        source.take_each('repeated_column_1, 'repeated_column_2', 'repeated_column_3') >> sink.put('repeated_column')

    This example would take a dataset like this:

    +---------------+-------------------+-------------------+-------------------+
    | normal_column | repeated_column_1 | repeated_column_2 | repeated_column_3 |
    +===============+===================+===================+===================+
    |       1       |         a         |         b         |         c         |
    +---------------+-------------------+-------------------+-------------------+
    |       2       |         d         |         e         |         f         |
    +---------------+-------------------+-------------------+-------------------+

    And make it look like this:

    +---------------+-----------------+
    | normal_column | repeated_column |
    +===============+=================+
    |       1       |        a        |
    +---------------+-----------------+
    |       1       |        b        |
    +---------------+-----------------+
    |       1       |        c        |
    +---------------+-----------------+
    |       2       |        d        |
    +---------------+-----------------+
    |       2       |        e        |
    +---------------+-----------------+
    |       2       |        f        |
    +---------------+-----------------+
    
    """
    def __init__(self, source:Source):
        """
        :param source: The source to wrap
        """
        self.source = source
        self._current_value = None
        self._current_value_counter = None
        self._max_value_counter = None
    
    def take_each(self, *keys, on_not_found=OnFail.fail) -> Source:
        self._max_value_counter = max(self._max_value_counter, len(keys)-1)
        return _RepeaterSourceTakeWrapper(self, tuple(self.take(key, on_not_found) for key in keys))

    def take_each_attr(self, *keys, on_not_found=OnFail.fail) -> Source:
        self._max_value_counter = max(self._max_value_counter, len(keys)-1)
        return _RepeaterSourceTakeWrapper(self, tuple(self.take_attr(key, on_not_found) for key in keys))
    
    def next(self):
        super().next()
        if self._current_value is None or self._current_value_counter == self._max_value_counter:
            # Advance to the actual next value
            self.source.idempotent_next(self._reset_idempotency)
            self._current_value = self.source.get()
            self._current_value_counter = 0
        else:
            # Advance the counter for the repeater
            self._current_value_counter += 1
    
    def get(self):
        return self.source.get()

    def get_index(self):
        index = self.source.get_index()
        if index is not None:
            return index, self._current_value_counter

    def check_progress(self):
        progress_completed, progress_total = self.source.check_progress()
        if progress_completed is not None:
            progress_completed *= self._max_value_counter
            progress_completed += self._current_value_counter
        if progress_total is not None:
            progress_total *= self._max_value_counter
        return progress_completed, progress_total

    def open(self):
        super().open()
        self.source.open()
    
    def close(self):
        super().close()
        self.source.close()


class _RepeaterSourceTakeWrapper(Source):
    def __init__(self, repeater_source:RepeaterSource, takes:Sequence[Source]):
        self.source = repeater_source
        self.takes = takes
    
    def get(self):
        try:
            take = self.takes[self.source._current_value_counter]
        except IndexError:
            return None
        return take.get()
    
    def __repr__(self):
        try:
            take = repr(self.takes[self.source._current_value_counter])
        except IndexError:
            take = ''
        return f"{repr(self.source)} take_each({take})"
        