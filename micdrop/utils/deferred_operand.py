__all__ = ('DeferredOperand',)

class DeferredOperand:
    """
    Returns an anonymous function that will mirror whatever operation is performed on this object.

    Can also be used with a callback to provide more control. Useful for building syntactic sugar.

    Example usage:

    >>> is_six = DeferredOperation() == 6
    >>> is_six(7)
    False
    >>> is_six(6)
    True
    >>> from functools import partial
    >>> is_six = DeferredOperation(lambda test: partial(filter, test)) == 6
    >>> list(is_six([6, 1, 2, 3, 4, 5, 6]))
    [6, 6]
    """
    def __init__(self, callback=None):
        self._callback = callback
    
    def _return(self, operation):
        if self._callback is None:
            return operation
        else:
            return self._callback(operation)
    
    def __lt__(self, other):
        return self._return(lambda val: val < other)

    def __le__(self, other):
        return self._return(lambda val: val <= other)

    def __eq__(self, other):
        return self._return(lambda val: val == other)

    def __ne__(self, other):
        return self._return(lambda val: val != other)

    def __gt__(self, other):
        return self._return(lambda val: val > other)

    def __ge__(self, other):
        return self._return(lambda val: val >= other)
    
    def __contains__(self, other):
        return self._return(lambda val: other in val)

    def __call__(self, *args, **kwargs):
        return self._return(lambda val: val(*args, **kwargs))

    def __add__(self, other):
        return self._return(lambda val: val + other)

    def __sub__(self, other):
        return self._return(lambda val: val - other)

    def __mul__(self, other):
        return self._return(lambda val: val * other)

    def __pow__(self, other, modulo=None):
        return self._return(lambda val: pow(val, other, modulo))

    def __truediv__(self, other):
        return self._return(lambda val: val / other)

    def __floordiv__(self, other):
        return self._return(lambda val: val // other)

    def __mod__(self, other):
        return self._return(lambda val: val % other)
    
    def __divmod__(self, other):
        return self._return(lambda val: divmod(val, other))

    def __lshift__(self, other):
        return self._return(lambda val: val << other)

    def __rshift__(self, other):
        return self._return(lambda val: val >> other)

    def __and__(self, other):
        return self._return(lambda val: val & other)

    def __or__(self, other):
        return self._return(lambda val: val | other)

    def __xor__(self, other):
        return self._return(lambda val: val ^ other)

    def __invert__(self):
        return self._return(lambda val: ~val)
    
    def __radd__(self, other):
        return self._return(lambda val: other + val)

    def __rsub__(self, other):
        return self._return(lambda val: other - val)

    def __rmul__(self, other):
        return self._return(lambda val: other * val)

    def __rtruediv__(self, other):
        return self._return(lambda val: other / val)

    def __rfloordiv__(self, other):
        return self._return(lambda val: other // val)

    def __rmod__(self, other):
        return self._return(lambda val: other % val)

    def __rdivmod__(self, other):
        return self._return(lambda val: divmod(other, val))

    def __rpow__(self, other, modulo=None):
        return self._return(lambda val: pow(other, val, modulo))

    def __rlshift__(self, other):
        return self._return(lambda val: other << val)

    def __rrshift__(self, other):
        return self._return(lambda val: other >> val)

    def __rand__(self, other):
        return self._return(lambda val: other & val)

    def __rxor__(self, other):
        return self._return(lambda val: other ^ val)

    def __ror__(self, other):
        return self._return(lambda val: other | val)
    
    def __neg__(self):
        return self._return(lambda val: -val)
    
    def __pos__(self):
        return self._return(lambda val: +val)
    
    def __abs__(self):
        return self._return(abs)
