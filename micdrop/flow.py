from __future__ import annotations
from typing import Callable
from .base import Put, PipelineItem, Source
from .collect import CollectArgsKwargsTakeMixin
from .utils import DeferredOperand
from .exceptions import SkipRow as SkipRowException, StopProcessing as StopProcessingException
__all__ = ('Choose', 'Branch', 'Coalesce', 'SkipRow', 'StopProcessing')

class Choose(PipelineItem):
    """
    Choose a single value from one of multiple component pipelines, based on the value of some condition pipeline.

    Usage::

        with source.take('conditional') >> Choose() as choice:
            source.take('col1') >> (choice.value == 6) # col2 will be used if conditional == 6
            source.take('col2') >> choice.check(lambda val: val > 6) # elif conditional > 6
            source.take('col3') >> choice.fallback() # else (e.g. conditional < 6)
            choice >> sink.put('chosen') 
            # chosen will have the value of either col1, col2, or col3 depending on the value of conditional
    """
    def __init__(self):
        self._branches = []
    
    def process(self, value):
        for condition, put in self._branches:
            if condition(value):
                return put.get()
        return None
    
    def idempotent_next(self, idempotency_counter):
        super().idempotent_next(idempotency_counter)
        for _, put in self._branches:
            put.idempotent_next(idempotency_counter)
    
    
    def check(self, condition:Callable):
        """
        This is a conditional put operation: the put will only receive the value if the condition
        returns `True` when called.

        :param condition: This callable will receive the value that is shifted up the pipeline into
            this Choice object, and should return `True` if this put's value is to be taken. If it
            returns `False`, we move on to checking the next conditional put.
        """
        put = Put()
        self._branches.append((condition, put))
        return put
    
    @property
    def value(self) -> DeferredOperand:
        """
        This special property allows you to specify simple check conditions using operator
        overloading.

        By way of example, this mean that ``(choice.value == 1)`` is syntactic sugar that is exactly
        equivalent to ``choice.check(lambda value: value == 1)``. (The parenthesis are usually 
        necessary because of operator precedence.)
        """
        return DeferredOperand(self.check)

    def is_in(self, container):
        return self.check(lambda val: val in container)
    
    def not_in(self, container):
        return self.check(lambda val: val not in container)
    
    def is_(self, other):
        return self.check(lambda val: val is other)
    
    def is_not(self, other):
        return self.check(lambda val: val is not other)
    
    def fallback(self):
        """
        A branch choice that will always be taken; equivalent to an else clause
        """
        return self.check(lambda _: True)

class Branch(Put):
    """
    Redirect put values down different pipelines, based on the value of some conditional pipeline.

    Usage::

        with source.take('conditional') >> Branch() as branch:
            source.take('col1') >> branch.put()
            source.take('col2') >> branch.put('named')
            with branch.value == 6 as case:
                # define pipelines for if conditional == 6
                case.take() >> sink.put('col1_six')
                case.take('named') >> sink.put('col2_six')
            with branch.value > 6 as case:
                # define pipelines for if conditional > 6
                case.take() >> sink.put('col1_bigger')
                case.take('named') >> sink.put('col2_bigger')
            with branch.fallback() as case:
                # define pipelines for if conditional < 6
                case.take() >> sink.put('col1_smaller')
                case.take('named') >> sink.put('col2_smaller')
            # The columns in one of the three above cases will be populated
            # (The puts in other cases will receive None)
            
    """
    def __init__(self):
        self._args = []
        self._kwargs = {}
        self._cases = []
        self._current_case = None
        self._cached = False
    
    def get(self, case:BranchCase):
        if not self._cached:
            value = super().get()
            for condition, condition_case in self._cases:
                if condition(value):
                    self._current_case = condition_case
                    break
            self._cached = True
        if case is self._current_case:
            return (
                [put.get() for put in self._args],
                {key: put.get() for key, put in self._kwargs.items()}
            )
        else:
            return (
                [None for _ in self._args],
                {key: None for key in self._kwargs.keys()}
            )
    
    def put(self, key=None):
        put = Put()
        if key is None:
            self._args.append(put)
        else:
            self._kwargs[key] = put
        return put
    
    def next(self):
        self._current_case = None
        self._cached = False

    def idempotent_next(self, idempotency_counter):
        super().idempotent_next(idempotency_counter)
        for put in self._args:
            put.idempotent_next(idempotency_counter)
        for put in self._kwargs.values():
            put.idempotent_next(idempotency_counter)
    
    def check(self, condition:Callable):
        """
        Create a fork in this branching pipeline based on the passed condition. When the condition
        is taken
        """
        case = BranchCase(self)
        self._cases.append((condition, case))
        return case
    
    @property
    def value(self) -> DeferredOperand:
        """
        This special property allows you to specify simple check conditions using operator
        overloading.

        By way of example, this mean that ``(choice.value == 1)`` is syntactic sugar that is exactly
        equivalent to ``choice.check(lambda value: value == 1)``. (The parenthesis are usually 
        necessary because of operator precedence.)
        """
        return DeferredOperand(self.check)

    def is_in(self, container):
        return self.check(lambda val: val in container)
    
    def not_in(self, container):
        return self.check(lambda val: val not in container)
    
    def is_(self, other):
        return self.check(lambda val: val is other)
    
    def is_not(self, other):
        return self.check(lambda val: val is not other)
    
    def fallback(self):
        """
        A branch choice that will always be taken; equivalent to an else clause
        """
        return self.check(lambda _: True)
    
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

class BranchCase(CollectArgsKwargsTakeMixin, Source):
    def __init__(self, branch:Branch):
        self._branch = branch
    
    def get(self):
        return self._branch.get(self)
    
    def idempotent_next(self, idempotency_counter):
        super().idempotent_next(idempotency_counter)
        self._branch.idempotent_next(idempotency_counter)


class Coalesce(Source):
    """
    A collector pipeline that returns the first non-null value that is put.
    """
    _value = None
    _cached = False
    def __init__(self, *pipelines:Source):
        self._puts = [item >> Put() for item in pipelines]
    
    def get(self):
        if not self._cached:
            for put in self._puts:
                value = put.get()
                if value is not None:
                    self._value = value
                    self._cached = True
                    break
            else:
                # all puts returned false
                self._value = None
                self._cached = True
        return self._value
    
    def next(self):
        self._value = None
        self._cached = False
    
    def put(self):
        put = Put()
        self._puts.append(put)
        return put


class SkipRow(Source):
    """
    Used with a `Choice` or `Branch` to skip the current row (e.g. if the source data represents something not supported in the target sink)

    Example::

        # This will only process rows where ``switch_me == 'good'``
        with source.take('switch_me') >> Choice() as choice:
            choice >> (choice.value == 'good')
            SkipRow >> choice.fallback()
    """
    def get(self):
        raise SkipRowException()


class StopProcessing(Source):
    """
    Used with a `Choice` or `Branch` to cleanly stop processing (e.g. this and all future rows will be skipped)
    """
    def get(self):
        raise StopProcessingException()